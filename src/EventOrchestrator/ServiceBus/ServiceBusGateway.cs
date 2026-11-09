using System.Text.Json;
using System.Transactions;
using Azure.Core;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace EventOrchestrator.ServiceBus;

public sealed record ServiceBusGatewayOptions
{
    public string OutboundQueue { get; init; } = "outbound";

    /// <summary>A queue only the orchestrator uses. Winners pass through it so the final publish can be one transaction.</summary>
    public string StagingQueue { get; init; } = "orchestrator-staging";

    /// <summary>How long a receive waits when the queue looks empty.</summary>
    public TimeSpan ReceiveWaitTime { get; init; } = TimeSpan.FromSeconds(5);

    public ServiceBusRetryOptions RetryOptions { get; init; } = new()
    {
        Mode = ServiceBusRetryMode.Exponential,
        MaxRetries = 5,
        Delay = TimeSpan.FromMilliseconds(500),
        MaxDelay = TimeSpan.FromSeconds(10),
        TryTimeout = TimeSpan.FromSeconds(30),
    };
}

/// <summary>
/// Azure Service Bus version of <see cref="IEventQueueGateway"/>.
///
/// Main design decisions:
/// <list type="bullet">
/// <item>PeekLock with PrefetchCount = 0. With prefetch on, the SDK pulls extra messages into a local
///   buffer and locks them, which breaks the "max 5 events" rule without anyone noticing.</item>
/// <item>One ServiceBusClient per source entity, each with EnableCrossEntityTransactions. In a
///   cross-entity transaction, every operation goes through the first entity the client used
///   (the "via" entity). A receive or complete can only be in a transaction on that entity, so
///   "complete on inbound-3 + send to outbound" works only on a client whose first operation was a
///   receive from inbound-3.</item>
/// <item>Service Bus cannot complete messages from three different queues in one transaction.
///   So each winner is first moved to a private staging queue (one transaction per source).
///   The final step is then a single transaction on staging: send all N to outbound in order and
///   complete the staged copies.</item>
/// <item>Events returned to a source queue get a new MessageId. If duplicate detection is on for that
///   queue, re-using the original MessageId would make Service Bus drop the copy silently.</item>
/// <item>Outbound messages use MessageId = EventId, so duplicate detection on the outbound queue
///   removes duplicates if a publish is retried.</item>
/// </list>
/// References:
///   https://learn.microsoft.com/azure/service-bus-messaging/service-bus-transactions
///   https://learn.microsoft.com/dotnet/api/azure.messaging.servicebus.servicebusclientoptions.enablecrossentitytransactions
///   https://learn.microsoft.com/azure/service-bus-messaging/service-bus-prefetch
///   https://github.com/Azure/azure-sdk-for-net/tree/main/sdk/servicebus/Azure.Messaging.ServiceBus/samples
/// </summary>
public sealed class ServiceBusGateway : IEventQueueGateway, IAsyncDisposable
{
    public const string EventIdProperty = "EventId";
    public const string OccurredAtProperty = "OccurredAtUtc";
    public const string SourceQueueProperty = "SourceQueue";
    public const string RunIdProperty = "OrchestratorRunId";

    private readonly Func<ServiceBusClientOptions, ServiceBusClient> _clientFactory;
    private readonly ServiceBusGatewayOptions _options;
    private readonly ILogger _log;
    private readonly string _runId = Guid.NewGuid().ToString("N");
    private readonly Dictionary<string, EntityChannel> _channels = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Recommended: Microsoft Entra ID (managed identity in Azure, developer login locally).
    /// The identity needs the "Azure Service Bus Data Owner" role, or Data Sender + Data Receiver, on the namespace.</summary>
    public ServiceBusGateway(string fullyQualifiedNamespace, TokenCredential credential,
        ServiceBusGatewayOptions? options = null, ILogger<ServiceBusGateway>? logger = null)
        : this(o => new ServiceBusClient(fullyQualifiedNamespace, credential, o), options, logger) { }

    /// <summary>For local emulator or test use only. Don't use key-based auth in production.</summary>
    public static ServiceBusGateway FromConnectionString(string connectionString,
        ServiceBusGatewayOptions? options = null, ILogger<ServiceBusGateway>? logger = null)
        => new(o => new ServiceBusClient(connectionString, o), options, logger);

    private ServiceBusGateway(Func<ServiceBusClientOptions, ServiceBusClient> clientFactory,
        ServiceBusGatewayOptions? options, ILogger<ServiceBusGateway>? logger)
    {
        _clientFactory = clientFactory;
        _options = options ?? new ServiceBusGatewayOptions();
        _log = logger ?? NullLogger<ServiceBusGateway>.Instance;
    }

    // ---------------------------------------------------------------- IEventQueueGateway

    public async Task<int> RecoverAsync(CancellationToken ct)
    {
        // Receive first, so staging becomes this client's "via" entity.
        var staging = Channel(_options.StagingQueue);
        int moved = 0;
        while (true)
        {
            var batch = await staging.Receiver.ReceiveMessagesAsync(5, _options.ReceiveWaitTime, ct);
            if (batch.Count == 0) return moved;

            foreach (var msg in batch)
            {
                if (!msg.ApplicationProperties.TryGetValue(SourceQueueProperty, out var src) || src is not string sourceQueue)
                {
                    await staging.Receiver.DeadLetterMessageAsync(msg, "MissingSourceQueue",
                        "Staged message has no SourceQueue property", ct);
                    continue;
                }
                using (var tx = NewTransaction())
                {
                    await staging.SenderTo(sourceQueue).SendMessageAsync(CopyForReturn(msg), ct);
                    await staging.Receiver.CompleteMessageAsync(msg, ct);
                    tx.Complete();
                }
                moved++;
            }
        }
    }

    public async Task<IReadOnlyList<ReceivedEvent>> ReceiveAsync(string queue, int maxMessages, CancellationToken ct)
    {
        var channel = Channel(queue);
        var messages = await channel.Receiver.ReceiveMessagesAsync(maxMessages, _options.ReceiveWaitTime, ct);
        return messages.Select(m => ToEvent(m, queue)).ToList();
    }

    public async Task ReturnToSourceAsync(ReceivedEvent e, CancellationToken ct)
    {
        var channel = Channel(e.SourceQueue);
        var msg = Msg(e);
        // Send and complete on the same entity in one transaction, so the event is never lost or duplicated.
        using var tx = NewTransaction();
        await channel.SenderTo(e.SourceQueue).SendMessageAsync(CopyForReturn(msg), ct);
        await channel.Receiver.CompleteMessageAsync(msg, ct);
        tx.Complete();
    }

    public Task CompleteAsync(ReceivedEvent e, CancellationToken ct)
        => Channel(e.SourceQueue).Receiver.CompleteMessageAsync(Msg(e), ct);

    public Task DeadLetterAsync(ReceivedEvent e, string reason, CancellationToken ct)
        => Channel(e.SourceQueue).Receiver.DeadLetterMessageAsync(Msg(e), "InvalidEvent", reason, ct);

    public Task AbandonAsync(ReceivedEvent e, CancellationToken ct)
        => Channel(e.SourceQueue).Receiver.AbandonMessageAsync(Msg(e), cancellationToken: ct);

    public async Task RenewLockAsync(ReceivedEvent e, CancellationToken ct)
    {
        var msg = Msg(e);
        await Channel(e.SourceQueue).Receiver.RenewMessageLockAsync(msg, ct);
        e.LockedUntil = msg.LockedUntil; // the SDK updates the received message in place
    }

    public async Task PublishAsync(IReadOnlyList<ReceivedEvent> oldestFirst, CancellationToken ct)
    {
        // Stage 1: move each winner from its source queue to staging, one transaction per winner
        // (the via entity is that source queue).
        foreach (var e in oldestFirst)
        {
            var channel = Channel(e.SourceQueue);
            var msg = Msg(e);
            var staged = new ServiceBusMessage(msg) { MessageId = $"{e.EventId}:{_runId}" };
            staged.ApplicationProperties[SourceQueueProperty] = e.SourceQueue;
            staged.ApplicationProperties[RunIdProperty] = _runId;

            using var tx = NewTransaction();
            await channel.SenderTo(_options.StagingQueue).SendMessageAsync(staged, ct);
            await channel.Receiver.CompleteMessageAsync(msg, ct);
            tx.Complete();
        }

        // Stage 2: one transaction on staging. Send all winners to outbound in order and complete the staged copies.
        var staging = Channel(_options.StagingQueue);
        var received = new List<ServiceBusReceivedMessage>(oldestFirst.Count);
        while (received.Count < oldestFirst.Count)
        {
            var batch = await staging.Receiver.ReceiveMessagesAsync(oldestFirst.Count - received.Count, _options.ReceiveWaitTime, ct);
            if (batch.Count == 0)
                throw new InvalidOperationException(
                    $"Staging held {received.Count} of {oldestFirst.Count} winners. Run recovery before retrying.");
            received.AddRange(batch);
        }

        var expectedIds = oldestFirst.Select(e => e.EventId).ToHashSet(StringComparer.Ordinal);
        var stranger = received.FirstOrDefault(m => !expectedIds.Contains(EventIdOf(m)) || !Equals(m.ApplicationProperties.GetValueOrDefault(RunIdProperty), _runId));
        if (stranger is not null)
        {
            foreach (var m in received) await staging.Receiver.AbandonMessageAsync(m, cancellationToken: ct);
            throw new InvalidOperationException(
                $"Staging contains a message ({EventIdOf(stranger)}) from a different run. Run recovery, then retry.");
        }

        var outbound = received
            .Select(m => (Msg: m, At: ToEvent(m, _options.StagingQueue)))
            .OrderBy(x => x.At, EventRanking.OldestFirst)
            .Select(x =>
            {
                var copy = new ServiceBusMessage(x.Msg) { MessageId = x.At.EventId }; // stable id, so duplicate detection on outbound works
                copy.ApplicationProperties.Remove(RunIdProperty);
                copy.ApplicationProperties.Remove(SourceQueueProperty);
                return copy;
            })
            .ToList();

        using (var tx = NewTransaction())
        {
            await staging.SenderTo(_options.OutboundQueue).SendMessagesAsync(outbound, ct); // one batch keeps the order
            foreach (var m in received)
                await staging.Receiver.CompleteMessageAsync(m, ct);
            tx.Complete();
        }
        _log.LogInformation("Published {Count} event(s) to {Outbound} in one transaction.", outbound.Count, _options.OutboundQueue);
    }

    // ---------------------------------------------------------------- helpers

    private EntityChannel Channel(string entity)
    {
        if (_channels.TryGetValue(entity, out var existing)) return existing;

        var client = _clientFactory(new ServiceBusClientOptions
        {
            EnableCrossEntityTransactions = true,
            RetryOptions = _options.RetryOptions,
        });
        var receiver = client.CreateReceiver(entity, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            PrefetchCount = 0, // REQUIRED: prefetch would lock messages we are not allowed to hold
        });
        var channel = new EntityChannel(entity, client, receiver);
        _channels[entity] = channel;
        return channel;
    }

    private static TransactionScope NewTransaction()
        => new(TransactionScopeOption.RequiresNew, TransactionScopeAsyncFlowOption.Enabled);

    private static ServiceBusReceivedMessage Msg(ReceivedEvent e) => (ServiceBusReceivedMessage)e.Handle;

    private ServiceBusMessage CopyForReturn(ServiceBusReceivedMessage original)
    {
        var copy = new ServiceBusMessage(original) { MessageId = Guid.NewGuid().ToString("N") };
        copy.ApplicationProperties[EventIdProperty] = EventIdOf(original);
        copy.ApplicationProperties[RunIdProperty] = _runId;
        copy.ApplicationProperties.Remove(SourceQueueProperty);
        return copy;
    }

    private static string EventIdOf(ServiceBusReceivedMessage m)
        => m.ApplicationProperties.TryGetValue(EventIdProperty, out var id) && id is string s && s.Length > 0 ? s : m.MessageId;

    /// <summary>
    /// Reads the business timestamp from the OccurredAtUtc application property, falling back to a JSON body field.
    /// We do not use EnqueuedTime: it is when the broker received the message (and it changes when an event
    /// is put back on a queue), not when the event happened.
    /// </summary>
    internal static ReceivedEvent ToEvent(ServiceBusReceivedMessage m, string queue)
    {
        DateTimeOffset? occurredAt = null;
        string? invalid = null;

        if (m.ApplicationProperties.TryGetValue(OccurredAtProperty, out var raw))
        {
            occurredAt = raw switch
            {
                DateTimeOffset dto => dto,
                DateTime dt => new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc)),
                string s when DateTimeOffset.TryParse(s, null, System.Globalization.DateTimeStyles.AssumeUniversal, out var p) => p,
                _ => null,
            };
            if (occurredAt is null) invalid = $"Unparseable {OccurredAtProperty}: '{raw}'";
        }
        else
        {
            try
            {
                using var doc = JsonDocument.Parse(m.Body.ToMemory());
                if (doc.RootElement.TryGetProperty("occurredAtUtc", out var el) && el.TryGetDateTimeOffset(out var p))
                    occurredAt = p;
                else invalid = $"No {OccurredAtProperty} property or body field";
            }
            catch (JsonException) { invalid = $"No {OccurredAtProperty} property and the body is not JSON"; }
        }

        return new ReceivedEvent
        {
            EventId = EventIdOf(m),
            OccurredAtUtc = occurredAt?.ToUniversalTime(),
            InvalidReason = invalid,
            SourceQueue = queue,
            LockedUntil = m.LockedUntil,
            Handle = m,
        };
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var c in _channels.Values) await c.DisposeAsync();
        _channels.Clear();
    }

    /// <summary>A client whose via entity is <see cref="Entity"/>, with its receiver and senders.</summary>
    private sealed class EntityChannel(string entity, ServiceBusClient client, ServiceBusReceiver receiver) : IAsyncDisposable
    {
        private readonly Dictionary<string, ServiceBusSender> _senders = new(StringComparer.OrdinalIgnoreCase);
        public string Entity { get; } = entity;
        public ServiceBusReceiver Receiver { get; } = receiver;

        public ServiceBusSender SenderTo(string target)
        {
            if (!_senders.TryGetValue(target, out var s)) _senders[target] = s = client.CreateSender(target);
            return s;
        }

        public async ValueTask DisposeAsync()
        {
            await Receiver.DisposeAsync();
            foreach (var s in _senders.Values) await s.DisposeAsync();
            await client.DisposeAsync();
        }
    }
}
