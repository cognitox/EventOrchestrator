namespace EventOrchestrator;

/// <summary>
/// An event that the orchestrator has received under a peek-lock.
/// It holds only what ranking needs, plus an opaque handle the gateway uses to settle the message.
/// </summary>
public sealed class ReceivedEvent
{
    public required string EventId { get; init; }

    /// <summary>When the business event happened. Null means the message has no valid timestamp.</summary>
    public DateTimeOffset? OccurredAtUtc { get; init; }

    /// <summary>Why <see cref="OccurredAtUtc"/> is null (used as the dead-letter reason).</summary>
    public string? InvalidReason { get; init; }

    /// <summary>The inbound queue the message was received from.</summary>
    public required string SourceQueue { get; init; }

    /// <summary>When the peek-lock expires. The orchestrator renews it if it gets close.</summary>
    public DateTimeOffset LockedUntil { get; set; }

    /// <summary>Gateway-specific settlement handle (e.g. ServiceBusReceivedMessage).</summary>
    public required object Handle { get; init; }

    public override string ToString() => $"{EventId} @ {OccurredAtUtc:yyyy-MM-dd HH:mm:ss.fff} (from {SourceQueue})";
}

/// <summary>What happens to events that are inspected but are not in the final Top N.</summary>
public enum LoserPolicy
{
    /// <summary>Re-publish to the tail of the source queue so no data is lost (default).</summary>
    ReturnToSource,

    /// <summary>Complete (delete) them. Use only if the business confirms they are disposable.</summary>
    Discard,
}

public sealed record OrchestratorOptions
{
    public IReadOnlyList<string> InboundQueues { get; init; } =
        ["inbound-1", "inbound-2", "inbound-3", "inbound-4", "inbound-5"];

    /// <summary>The task guarantees exactly 5 events per queue, so we read exactly that many.
    /// Counting also stops us re-reading losers we put back on the tail of the same queue.</summary>
    public int EventsPerQueue { get; init; } = 5;

    public int TopN { get; init; } = 3;

    /// <summary>The most events the orchestrator may hold at once (received but not yet settled).</summary>
    public int MaxEventsInFlight { get; init; } = 5;

    public LoserPolicy LoserPolicy { get; init; } = LoserPolicy.ReturnToSource;

    /// <summary>Renew a held lock if it expires within this window.</summary>
    public TimeSpan LockRenewalMargin { get; init; } = TimeSpan.FromSeconds(20);

    public void Validate()
    {
        if (InboundQueues.Count == 0) throw new ArgumentException("At least one inbound queue is required.");
        if (TopN < 1) throw new ArgumentException("TopN must be at least 1.");
        if (MaxEventsInFlight <= TopN)
            throw new ArgumentException(
                $"MaxEventsInFlight ({MaxEventsInFlight}) must be greater than TopN ({TopN}), " +
                "otherwise there is no free slot to receive new events while holding the candidates.");
        if (EventsPerQueue < 0) throw new ArgumentException("EventsPerQueue cannot be negative.");
    }
}

public sealed record RunReport(
    IReadOnlyList<ReceivedEvent> Published,
    int ReceiveCalls,
    int EventsInspected,
    int MaxEventsInFlight,
    int ReturnedToSource,
    int Discarded,
    int DeadLettered,
    int RecoveredFromStaging);

/// <summary>Defines "most recent": event time first, then EventId as a deterministic tie-breaker.</summary>
public static class EventRanking
{
    public static readonly IComparer<ReceivedEvent> OldestFirst = Comparer<ReceivedEvent>.Create((a, b) =>
    {
        int byTime = Nullable.Compare(a.OccurredAtUtc, b.OccurredAtUtc);
        return byTime != 0 ? byTime : string.CompareOrdinal(a.EventId, b.EventId);
    });
}
