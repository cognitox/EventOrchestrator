using System.Text.Json;
using Azure.Identity;
using Azure.Messaging.ServiceBus;
using EventOrchestrator;
using EventOrchestrator.InMemory;
using EventOrchestrator.ServiceBus;
using Microsoft.Extensions.Logging;

// Usage:
//   dotnet run                                                    offline demo with the in-memory broker
//   dotnet run -- --namespace <ns>.servicebus.windows.net [--seed] Azure Service Bus with Entra ID (recommended)
//   dotnet run -- --connection-string "<cs>" [--seed]             Service Bus emulator or a test namespace
//   --seed     first sends 5 random events to each inbound queue
//   --discard  completes the losers instead of returning them to their source queue

using var loggerFactory = LoggerFactory.Create(b => b.AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss "; }));
var options = new OrchestratorOptions
{
    LoserPolicy = args.Contains("--discard") ? LoserPolicy.Discard : LoserPolicy.ReturnToSource,
};
string? Arg(string name) => Array.IndexOf(args, name) is var i and >= 0 && i + 1 < args.Length ? args[i + 1] : null;

var ns = Arg("--namespace");
var cs = Arg("--connection-string");

if (ns is null && cs is null)
{
    // ---------------- Offline demo
    var broker = new InMemoryGateway();
    var rng = new Random(args.Contains("--fixed") ? 42 : Environment.TickCount);
    var start = new DateTimeOffset(2026, 1, 1, 9, 0, 0, TimeSpan.Zero);
    Console.WriteLine("Inbound queues (front -> back), times as mm:ss after 09:00:");
    foreach (var q in options.InboundQueues)
    {
        var line = new List<string>();
        for (int i = 1; i <= options.EventsPerQueue; i++)
        {
            var at = start.AddSeconds(rng.Next(0, 3600));
            broker.Seed(q, $"{q[^1]}-{i}", at);
            line.Add($"{q[^1]}-{i}@{at:mm:ss}");
        }
        Console.WriteLine($"  {q}: {string.Join("  ", line)}");
    }

    var report = await new Orchestrator(broker, options, loggerFactory.CreateLogger<Orchestrator>()).RunAsync();

    Console.WriteLine();
    Console.WriteLine("Outbound queue (oldest first):");
    foreach (var m in broker.Contents(broker.Outbound)) Console.WriteLine($"  {m.EventId} @ {m.OccurredAtUtc:mm:ss}");
    Console.WriteLine();
    Console.WriteLine($"Receive calls (inbound)   : {report.ReceiveCalls}");
    Console.WriteLine($"Events inspected          : {report.EventsInspected}");
    Console.WriteLine($"Max events held (orch.)   : {report.MaxEventsInFlight}");
    Console.WriteLine($"Max events locked (broker): {broker.MaxLocked}");
    Console.WriteLine($"Returned to source        : {report.ReturnedToSource}");
    Console.WriteLine($"Left on inbound queues    : {options.InboundQueues.Sum(q => broker.Contents(q).Count)}");
    return 0;
}

// ---------------- Azure Service Bus
await using var gateway = ns is not null
    ? new ServiceBusGateway(ns, new DefaultAzureCredential(), logger: loggerFactory.CreateLogger<ServiceBusGateway>())
    : ServiceBusGateway.FromConnectionString(cs!, logger: loggerFactory.CreateLogger<ServiceBusGateway>());

if (args.Contains("--seed"))
{
    await using var seedClient = ns is not null ? new ServiceBusClient(ns, new DefaultAzureCredential()) : new ServiceBusClient(cs);
    var rng = new Random();
    foreach (var q in options.InboundQueues)
    {
        await using var sender = seedClient.CreateSender(q);
        var batch = Enumerable.Range(1, options.EventsPerQueue).Select(i =>
        {
            var id = $"{q}-{i}-{Guid.NewGuid():N}"[..20];
            var at = DateTimeOffset.UtcNow.AddSeconds(-rng.Next(0, 3600));
            var msg = new ServiceBusMessage(JsonSerializer.SerializeToUtf8Bytes(new { eventId = id, occurredAtUtc = at }))
            {
                MessageId = id,
                ContentType = "application/json",
            };
            msg.ApplicationProperties[ServiceBusGateway.EventIdProperty] = id;
            msg.ApplicationProperties[ServiceBusGateway.OccurredAtProperty] = at;
            return msg;
        });
        await sender.SendMessagesAsync(batch);
        Console.WriteLine($"Seeded {options.EventsPerQueue} events to {q}");
    }
}

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

var result = await new Orchestrator(gateway, options, loggerFactory.CreateLogger<Orchestrator>()).RunAsync(cts.Token);
Console.WriteLine("Published (oldest first):");
foreach (var e in result.Published) Console.WriteLine($"  {e}");
Console.WriteLine($"Receive calls: {result.ReceiveCalls}, inspected: {result.EventsInspected}, max held: {result.MaxEventsInFlight}");
return 0;
