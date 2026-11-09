using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace EventOrchestrator;

/// <summary>
/// Finds the N most recent events across the inbound queues and publishes them to the outbound
/// queue in chronological order, never holding more than <see cref="OrchestratorOptions.MaxEventsInFlight"/>
/// events at once.
///
/// Algorithm (streaming Top-N):
///   * Hold at most N candidates. Each receive fills only the free slots (5, then 5 - 3 = 2).
///   * After each receive, sort the candidates together with the new batch (at most 5) and keep the newest N.
///     Settle the events that fall out right away, so they stop counting against the limit.
///   * Candidates stay locked (not completed) until the final publish, so a crash loses nothing.
/// For 5 queues x 5 events: 1 + 4 x 3 = 13 receive calls, 25 events inspected, at most 5 held.
/// </summary>
public sealed class Orchestrator
{
    private readonly IEventQueueGateway _gateway;
    private readonly OrchestratorOptions _options;
    private readonly ILogger _log;
    private readonly TimeProvider _clock;

    public Orchestrator(IEventQueueGateway gateway, OrchestratorOptions options,
        ILogger<Orchestrator>? logger = null, TimeProvider? clock = null)
    {
        options.Validate();
        _gateway = gateway;
        _options = options;
        _log = logger ?? NullLogger<Orchestrator>.Instance;
        _clock = clock ?? TimeProvider.System;
    }

    public async Task<RunReport> RunAsync(CancellationToken ct = default)
    {
        int recovered = await _gateway.RecoverAsync(ct);
        if (recovered > 0)
            _log.LogWarning("Returned {Count} event(s) from staging to their source queues (previous run did not finish).", recovered);

        var candidates = new List<ReceivedEvent>(_options.MaxEventsInFlight); // the current Top-N, oldest first
        var inFlight = new HashSet<ReceivedEvent>();                          // everything locked and not yet settled
        int receiveCalls = 0, inspected = 0, maxInFlight = 0, returned = 0, discarded = 0, deadLettered = 0;

        try
        {
            foreach (var queue in _options.InboundQueues)
            {
                int remaining = _options.EventsPerQueue;
                while (remaining > 0)
                {
                    await RenewExpiringLocksAsync(inFlight, ct);

                    // Fill only the free slots: 5 on the first receive, then 5 - N.
                    int ask = Math.Min(_options.MaxEventsInFlight - inFlight.Count, remaining);
                    var batch = await _gateway.ReceiveAsync(queue, ask, ct);
                    receiveCalls++;

                    if (batch.Count == 0)
                    {
                        _log.LogWarning("{Queue}: expected {Remaining} more event(s) but the queue is empty; moving on.", queue, remaining);
                        break;
                    }
                    if (batch.Count > ask)
                        throw new InvalidOperationException($"Gateway returned {batch.Count} messages when {ask} were requested.");

                    remaining -= batch.Count;
                    inspected += batch.Count;
                    inFlight.UnionWith(batch);
                    maxInFlight = Math.Max(maxInFlight, inFlight.Count);

                    foreach (var e in batch)
                    {
                        if (e.OccurredAtUtc is null)
                        {
                            // Poison message: never let it take up a slot or block the run.
                            await _gateway.DeadLetterAsync(e, e.InvalidReason ?? "Missing OccurredAtUtc", ct);
                            inFlight.Remove(e);
                            deadLettered++;
                            _log.LogWarning("{Queue}: dead-lettered {EventId}: {Reason}", queue, e.EventId, e.InvalidReason);
                            continue;
                        }
                        candidates.Add(e);
                    }

                    // Keep the newest N. With at most 5 events this simple sort is the clearest option.
                    candidates.Sort(EventRanking.OldestFirst);
                    int excess = candidates.Count - _options.TopN;
                    if (excess <= 0) continue;

                    var losers = candidates.GetRange(0, excess);
                    candidates.RemoveRange(0, excess);
                    foreach (var loser in losers)
                    {
                        await SettleLoserAsync(loser, ct);
                        inFlight.Remove(loser);
                        if (_options.LoserPolicy == LoserPolicy.ReturnToSource) returned++; else discarded++;
                    }
                }
            }

            await RenewExpiringLocksAsync(inFlight, ct);
            if (candidates.Count > 0)
            {
                await _gateway.PublishAsync(candidates, ct); // candidates are already oldest-first
                inFlight.Clear();
            }

            _log.LogInformation("Published {Count} event(s) with {Receives} receive call(s); {Inspected} inspected; at most {Max} held.",
                candidates.Count, receiveCalls, inspected, maxInFlight);

            return new RunReport(candidates, receiveCalls, inspected, maxInFlight, returned, discarded, deadLettered, recovered);
        }
        catch (Exception ex)
        {
            // Best effort: release our locks so the events become available again straight away
            // and don't wait for the locks to expire. If this fails too, lock expiry does the same job.
            _log.LogError(ex, "Run failed; abandoning {Count} held event(s).", inFlight.Count);
            foreach (var e in inFlight)
            {
                try { await _gateway.AbandonAsync(e, CancellationToken.None); }
                catch (Exception abandonEx) { _log.LogWarning(abandonEx, "Could not abandon {EventId}; its lock will expire.", e.EventId); }
            }
            throw;
        }
    }

    private Task SettleLoserAsync(ReceivedEvent loser, CancellationToken ct) => _options.LoserPolicy switch
    {
        LoserPolicy.ReturnToSource => _gateway.ReturnToSourceAsync(loser, ct),
        LoserPolicy.Discard => _gateway.CompleteAsync(loser, ct),
        _ => throw new ArgumentOutOfRangeException(nameof(_options.LoserPolicy)),
    };

    private async Task RenewExpiringLocksAsync(IEnumerable<ReceivedEvent> held, CancellationToken ct)
    {
        var threshold = _clock.GetUtcNow() + _options.LockRenewalMargin;
        foreach (var e in held)
            if (e.LockedUntil <= threshold)
                await _gateway.RenewLockAsync(e, ct);
    }
}
