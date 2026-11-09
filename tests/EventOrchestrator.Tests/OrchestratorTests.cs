using EventOrchestrator.InMemory;

namespace EventOrchestrator.Tests;

public class OrchestratorTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 9, 0, 0, TimeSpan.Zero);
    private static readonly OrchestratorOptions Options = new();

    /// <summary>Seeds 5 queues x 5 events with random times. Returns every (id, time) for the brute-force check.</summary>
    private static List<(string Id, DateTimeOffset At)> SeedRandom(InMemoryGateway broker, int seed)
    {
        var rng = new Random(seed);
        var all = new List<(string, DateTimeOffset)>();
        foreach (var q in Options.InboundQueues)
            for (int i = 1; i <= 5; i++)
            {
                // On even seeds a narrow time range causes many ties, which tests the tie-breaker.
                var at = T0.AddSeconds(rng.Next(0, seed % 2 == 0 ? 30 : 100_000));
                var id = $"{q}-{i}";
                broker.Seed(q, id, at);
                all.Add((id, at));
            }
        return all;
    }

    private static List<string> BruteForceTop3OldestFirst(IEnumerable<(string Id, DateTimeOffset At)> all) => all
        .OrderByDescending(e => e.At).ThenByDescending(e => e.Id, StringComparer.Ordinal)
        .Take(3)
        .OrderBy(e => e.At).ThenBy(e => e.Id, StringComparer.Ordinal)
        .Select(e => e.Id).ToList();

    private static List<string> Ids(IEnumerable<InMemoryGateway.Stored> msgs) => msgs.Select(m => m.EventId).ToList();

    public static TheoryData<int> Seeds()
    {
        var d = new TheoryData<int>();
        for (int s = 1; s <= 300; s++) d.Add(s);
        return d;
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task Publishes_the_three_most_recent_in_chronological_order(int seed)
    {
        var broker = new InMemoryGateway();
        var all = SeedRandom(broker, seed);

        var report = await new Orchestrator(broker, Options).RunAsync();

        Assert.Equal(BruteForceTop3OldestFirst(all), Ids(broker.Contents(broker.Outbound)));
        Assert.Equal(13, report.ReceiveCalls);   // 1 x 5, then 2 + 2 + 1 for each of the other 4 queues
        Assert.Equal(25, report.EventsInspected);
        Assert.True(broker.MaxLocked <= 5, $"broker saw {broker.MaxLocked} locked");
        Assert.Equal(5, report.MaxEventsInFlight);
        Assert.Equal(0, broker.LockedCount);
        Assert.Empty(broker.Contents(broker.Staging));
    }

    [Fact]
    public async Task No_event_is_lost_or_duplicated_when_losers_are_returned()
    {
        var broker = new InMemoryGateway();
        var all = SeedRandom(broker, 7);

        var report = await new Orchestrator(broker, Options).RunAsync();

        var everywhere = Options.InboundQueues.SelectMany(q => Ids(broker.Contents(q)))
            .Concat(Ids(broker.Contents(broker.Outbound))).OrderBy(x => x).ToList();
        Assert.Equal(all.Select(a => a.Id).OrderBy(x => x), everywhere);
        Assert.Equal(22, report.ReturnedToSource);
        foreach (var q in Options.InboundQueues)                    // each loser goes back to its own queue
            Assert.All(broker.Contents(q), m => Assert.StartsWith(q, m.EventId));
    }

    [Fact]
    public async Task Discard_policy_empties_inbound_queues()
    {
        var broker = new InMemoryGateway();
        SeedRandom(broker, 11);

        var report = await new Orchestrator(broker, Options with { LoserPolicy = LoserPolicy.Discard }).RunAsync();

        Assert.All(Options.InboundQueues, q => Assert.Empty(broker.Contents(q)));
        Assert.Equal(22, report.Discarded);
        Assert.Equal(3, broker.Contents(broker.Outbound).Count);
    }

    [Fact]
    public async Task Queue_order_is_not_time_order_so_every_event_must_be_inspected()
    {
        var broker = new InMemoryGateway();
        for (int i = 0; i < 5; i++) broker.Seed("inbound-1", $"a{i}", T0.AddMinutes(100 - i * 2));       // newest first: 100, 98, ...
        for (int q = 2; q <= 4; q++) for (int i = 0; i < 5; i++) broker.Seed($"inbound-{q}", $"m{q}{i}", T0.AddMinutes(i));
        for (int i = 0; i < 5; i++) broker.Seed("inbound-5", $"z{i}", T0.AddMinutes(90 + i * 3));        // oldest first: 90 .. 102

        await new Orchestrator(broker, Options).RunAsync();

        Assert.Equal(["z3", "a0", "z4"], Ids(broker.Contents(broker.Outbound)));  // 99, 100, 102
    }

    [Fact]
    public async Task Equal_timestamps_are_broken_by_event_id()
    {
        var broker = new InMemoryGateway();
        foreach (var q in Options.InboundQueues)
            for (int i = 0; i < 5; i++) broker.Seed(q, $"{q}-{i}", T0);

        await new Orchestrator(broker, Options).RunAsync();

        Assert.Equal(["inbound-5-2", "inbound-5-3", "inbound-5-4"], Ids(broker.Contents(broker.Outbound)));
    }

    [Fact]
    public async Task Poison_message_is_dead_lettered_and_does_not_take_a_slot()
    {
        var reference = new InMemoryGateway();
        var all = SeedRandom(reference, 3);

        var broker = new InMemoryGateway();
        foreach (var (id, at) in all)
        {
            var queue = id[..9]; // "inbound-N"
            if (id == "inbound-4-1") broker.Seed(queue, "bad", null, "Unparseable OccurredAtUtc: 'yesterday'");
            else broker.Seed(queue, id, at);
        }

        var report = await new Orchestrator(broker, Options).RunAsync();

        Assert.Equal(1, report.DeadLettered);
        Assert.Equal("bad", Assert.Single(broker.DeadLetters).Msg.EventId);
        Assert.Equal(BruteForceTop3OldestFirst(all.Where(a => a.Id != "inbound-4-1")), Ids(broker.Contents(broker.Outbound)));
        Assert.True(broker.MaxLocked <= 5);
    }

    [Fact]
    public async Task Fewer_events_than_top_n_publishes_what_exists()
    {
        var broker = new InMemoryGateway();
        broker.Seed("inbound-3", "b", T0.AddMinutes(2));
        broker.Seed("inbound-1", "a", T0.AddMinutes(5));

        var report = await new Orchestrator(broker, Options).RunAsync();

        Assert.Equal(["b", "a"], Ids(broker.Contents(broker.Outbound)));
        Assert.Equal(2, report.Published.Count);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task Crash_during_publish_recovers_without_loss_and_gives_the_same_answer(int stagedBeforeCrash)
    {
        var broker = new InMemoryGateway { FailPublishAfterStaging = stagedBeforeCrash };
        var all = SeedRandom(broker, 21);

        await Assert.ThrowsAsync<InvalidOperationException>(() => new Orchestrator(broker, Options).RunAsync());
        Assert.Empty(broker.Contents(broker.Outbound));   // downstream never sees a partial result
        broker.SimulateCrash();                           // any locks still held expire

        var report = await new Orchestrator(broker, Options).RunAsync();

        Assert.Equal(stagedBeforeCrash, report.RecoveredFromStaging);
        Assert.Equal(BruteForceTop3OldestFirst(all), Ids(broker.Contents(broker.Outbound)));
        var everywhere = Options.InboundQueues.SelectMany(q => Ids(broker.Contents(q))).Concat(Ids(broker.Contents(broker.Outbound)));
        Assert.Equal(all.Select(a => a.Id).OrderBy(x => x), everywhere.OrderBy(x => x));
    }

    [Fact]
    public void Options_reject_a_limit_that_leaves_no_room_to_receive()
    {
        Assert.Throws<ArgumentException>(() => new Orchestrator(new InMemoryGateway(), Options with { MaxEventsInFlight = 3 }));
    }
}
