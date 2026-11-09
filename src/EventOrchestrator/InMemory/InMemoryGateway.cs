namespace EventOrchestrator.InMemory;

/// <summary>
/// An in-memory broker with Service Bus peek-lock semantics, used for tests and the offline demo.
/// It checks the "max events in flight" rule itself (locked and not yet settled), so tests
/// do not have to trust the orchestrator's own count.
/// </summary>
public sealed class InMemoryGateway(int maxInFlight = 5, string outbound = "outbound", string staging = "orchestrator-staging")
    : IEventQueueGateway
{
    public sealed class Stored
    {
        public required long Seq { get; init; }
        public required string EventId { get; init; }
        public DateTimeOffset? OccurredAtUtc { get; init; }
        public string? InvalidReason { get; init; }
        public string? StagedFrom { get; set; }
        public bool Locked { get; set; }
    }

    private readonly Dictionary<string, List<Stored>> _queues = new(StringComparer.OrdinalIgnoreCase);
    private long _seq;

    public string Outbound { get; } = outbound;
    public string Staging { get; } = staging;
    public List<(Stored Msg, string Queue, string Reason)> DeadLetters { get; } = [];

    // Metrics
    public int ReceiveCalls { get; private set; }
    public int Sends { get; private set; }
    public int Completes { get; private set; }
    public int MaxLocked { get; private set; }

    /// <summary>Fault injection: throw during PublishAsync after this many winners have been staged.</summary>
    public int? FailPublishAfterStaging { get; set; }

    public int LockedCount => _queues.Values.Sum(q => q.Count(m => m.Locked));

    public void Seed(string queue, string eventId, DateTimeOffset? occurredAtUtc, string? invalidReason = null)
        => Q(queue).Add(new Stored { Seq = ++_seq, EventId = eventId, OccurredAtUtc = occurredAtUtc, InvalidReason = invalidReason });

    public IReadOnlyList<Stored> Contents(string queue) => Q(queue).OrderBy(m => m.Seq).ToList();

    /// <summary>Simulates the process dying: every lock expires and the messages become visible again.</summary>
    public void SimulateCrash()
    {
        foreach (var m in _queues.Values.SelectMany(q => q)) m.Locked = false;
        FailPublishAfterStaging = null;
    }

    // ---------------------------------------------------------------- IEventQueueGateway

    public Task<int> RecoverAsync(CancellationToken ct)
    {
        int moved = 0;
        foreach (var m in Q(Staging).Where(m => !m.Locked).ToList())
        {
            Q(Staging).Remove(m);
            Q(m.StagedFrom!).Add(Clone(m, stagedFrom: null));
            Sends++; Completes++; moved++;
        }
        return Task.FromResult(moved);
    }

    public Task<IReadOnlyList<ReceivedEvent>> ReceiveAsync(string queue, int maxMessages, CancellationToken ct)
    {
        if (maxMessages < 1) throw new ArgumentOutOfRangeException(nameof(maxMessages));
        if (LockedCount + maxMessages > maxInFlight)
            throw new InvalidOperationException(
                $"Constraint violated: {LockedCount} held + {maxMessages} requested > {maxInFlight}.");

        ReceiveCalls++;
        var batch = Q(queue).Where(m => !m.Locked).OrderBy(m => m.Seq).Take(maxMessages).ToList();
        foreach (var m in batch) m.Locked = true;
        MaxLocked = Math.Max(MaxLocked, LockedCount);

        IReadOnlyList<ReceivedEvent> result = batch.Select(m => new ReceivedEvent
        {
            EventId = m.EventId,
            OccurredAtUtc = m.OccurredAtUtc,
            InvalidReason = m.OccurredAtUtc is null ? m.InvalidReason ?? "Missing OccurredAtUtc" : null,
            SourceQueue = queue,
            LockedUntil = DateTimeOffset.UtcNow.AddMinutes(1),
            Handle = m,
        }).ToList();
        return Task.FromResult(result);
    }

    public Task ReturnToSourceAsync(ReceivedEvent e, CancellationToken ct)
    {
        var m = Settle(e);
        Q(e.SourceQueue).Add(Clone(m, stagedFrom: null)); // new sequence number = tail of the queue
        Sends++; Completes++;
        return Task.CompletedTask;
    }

    public Task CompleteAsync(ReceivedEvent e, CancellationToken ct)
    {
        Settle(e); Completes++;
        return Task.CompletedTask;
    }

    public Task DeadLetterAsync(ReceivedEvent e, string reason, CancellationToken ct)
    {
        DeadLetters.Add((Settle(e), e.SourceQueue, reason));
        return Task.CompletedTask;
    }

    public Task AbandonAsync(ReceivedEvent e, CancellationToken ct)
    {
        var m = Locked(e);
        m.Locked = false; // keeps its original sequence number and position
        return Task.CompletedTask;
    }

    public Task RenewLockAsync(ReceivedEvent e, CancellationToken ct)
    {
        Locked(e);
        e.LockedUntil = DateTimeOffset.UtcNow.AddMinutes(1);
        return Task.CompletedTask;
    }

    public Task PublishAsync(IReadOnlyList<ReceivedEvent> oldestFirst, CancellationToken ct)
    {
        // Stage 1: move each winner to staging (one transaction per source queue in Service Bus).
        int staged = 0;
        foreach (var e in oldestFirst)
        {
            if (FailPublishAfterStaging == staged)
                throw new InvalidOperationException($"Injected failure after staging {staged} winner(s).");
            var m = Settle(e);
            Q(Staging).Add(Clone(m, stagedFrom: e.SourceQueue));
            Sends++; Completes++; staged++;
        }

        // Stage 2: one atomic move from staging to outbound, oldest first.
        ReceiveCalls++;
        var stagedMsgs = Q(Staging).OrderBy(m => m.Seq).ToList();
        foreach (var m in stagedMsgs.OrderBy(m => m.OccurredAtUtc).ThenBy(m => m.EventId, StringComparer.Ordinal))
            Q(Outbound).Add(Clone(m, stagedFrom: null));
        Q(Staging).Clear();
        Sends += stagedMsgs.Count; Completes += stagedMsgs.Count;
        return Task.CompletedTask;
    }

    // ---------------------------------------------------------------- helpers

    private List<Stored> Q(string name)
    {
        if (!_queues.TryGetValue(name, out var q)) _queues[name] = q = [];
        return q;
    }

    private Stored Locked(ReceivedEvent e)
    {
        var m = (Stored)e.Handle;
        if (!m.Locked || !Q(e.SourceQueue).Contains(m))
            throw new InvalidOperationException($"Lock lost for {e.EventId} (MessageLockLost).");
        return m;
    }

    private Stored Settle(ReceivedEvent e)
    {
        var m = Locked(e);
        Q(e.SourceQueue).Remove(m);
        return m;
    }

    private Stored Clone(Stored m, string? stagedFrom) => new()
    {
        Seq = ++_seq,
        EventId = m.EventId,
        OccurredAtUtc = m.OccurredAtUtc,
        InvalidReason = m.InvalidReason,
        StagedFrom = stagedFrom,
    };
}
