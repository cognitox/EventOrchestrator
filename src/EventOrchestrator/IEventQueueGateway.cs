namespace EventOrchestrator;

/// <summary>
/// The broker operations the orchestrator needs. The contract follows Azure Service Bus peek-lock
/// semantics: a received message is locked (invisible to others) until it is settled
/// (completed, abandoned, dead-lettered) or its lock expires.
/// </summary>
public interface IEventQueueGateway
{
    /// <summary>
    /// Returns events left in the staging queue by a run that crashed to their source queues.
    /// Call this before a run starts. Returns how many it moved.
    /// </summary>
    Task<int> RecoverAsync(CancellationToken ct);

    /// <summary>
    /// Receives up to <paramref name="maxMessages"/> messages in peek-lock mode. Must never buffer
    /// more than requested (no prefetch). May return fewer, and returns none when the queue is empty.
    /// </summary>
    Task<IReadOnlyList<ReceivedEvent>> ReceiveAsync(string queue, int maxMessages, CancellationToken ct);

    /// <summary>Atomically re-enqueues a copy at the tail of its source queue and completes the original.</summary>
    Task ReturnToSourceAsync(ReceivedEvent e, CancellationToken ct);

    /// <summary>Completes (deletes) the message.</summary>
    Task CompleteAsync(ReceivedEvent e, CancellationToken ct);

    /// <summary>Moves a message that cannot be processed to the dead-letter sub-queue.</summary>
    Task DeadLetterAsync(ReceivedEvent e, string reason, CancellationToken ct);

    /// <summary>Releases the lock so the message is available again straight away, in its original position.</summary>
    Task AbandonAsync(ReceivedEvent e, CancellationToken ct);

    /// <summary>Extends the peek-lock and updates <see cref="ReceivedEvent.LockedUntil"/>.</summary>
    Task RenewLockAsync(ReceivedEvent e, CancellationToken ct);

    /// <summary>
    /// Publishes the winners to the outbound queue in the order given, all or nothing.
    /// If this fails part-way, every winner is either still in its source queue or in staging,
    /// and <see cref="RecoverAsync"/> puts the staged ones back.
    /// </summary>
    Task PublishAsync(IReadOnlyList<ReceivedEvent> oldestFirst, CancellationToken ct);
}
