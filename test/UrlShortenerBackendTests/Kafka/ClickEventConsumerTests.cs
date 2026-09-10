using System.Text.Json;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Kafka.Events;
using UrlShortenerBackend.Api.Models;
using UrlShortenerBackend.Tests.Integration;

namespace UrlShortenerBackend.Tests.Kafka;

[Collection("Kafka tests")]
public class ClickEventConsumerTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture _postgres;

    public ClickEventConsumerTests(PostgresFixture postgres)
    {
        _postgres = postgres;
    }

    private UrlShortenerDbContext CreateDbContext()
    {
        var options =
            new DbContextOptionsBuilder<UrlShortenerDbContext>()
                .UseNpgsql(_postgres.ConnectionString)
                .Options;

        return new UrlShortenerDbContext(options);
    }

    [Fact]
    public async Task Consumer_ProcessesPublishedClickEvent()
    {
        var shortCode =
            $"test{Guid.NewGuid():N}"[..6];

        var eventId = Guid.NewGuid();

        await using (var context = CreateDbContext())
        {
            context.Urls.Add(
                new Url
                {
                    OriginalUrl = "https://www.example.com",
                    ShortCode = shortCode,
                    CreatedAt = DateTime.UtcNow,
                    ClickCount = 0
                });

            await context.SaveChangesAsync();
        }

        using var factory = new KafkaTestFactory(_postgres);

        using var client = factory.CreateClient();

        var clickEvent =
            new UrlClickedEvent(
                eventId,
                shortCode,
                DateTime.UtcNow);

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9093"
        };

        using var producer =
            new ProducerBuilder<string, string>(
                producerConfig)
                .Build();

        await producer.ProduceAsync(
            "url-clicked",
            new Message<string, string>
            {
                Key = shortCode,
                Value = JsonSerializer.Serialize(clickEvent)
            });

        await WaitForClickCountAsync(
            shortCode,
            expectedCount: 1);

        await using var verificationContext =
            CreateDbContext();

        var url =
            await verificationContext.Urls
                .SingleAsync(
                    x => x.ShortCode == shortCode);

        Assert.Equal(1, url.ClickCount);

        var processedEvent =
            await verificationContext.ProcessedClickEvents
                .SingleAsync(
                    x => x.EventId == eventId);

        Assert.Equal(eventId, processedEvent.EventId);
    }

    private async Task WaitForClickCountAsync(
        string shortCode,
        int expectedCount,
        int timeoutSeconds = 10)
    {
        var timeout =
            TimeSpan.FromSeconds(timeoutSeconds);

        var startedAt = DateTime.UtcNow;

        while (DateTime.UtcNow - startedAt < timeout)
        {
            await using var context =
                CreateDbContext();

            var clickCount =
                await context.Urls
                    .Where(x => x.ShortCode == shortCode)
                    .Select(x => x.ClickCount)
                    .SingleAsync();

            if (clickCount == expectedCount)
            {
                return;
            }

            await Task.Delay(100);
        }

        throw new TimeoutException(
            $"Timed out waiting for short code '{shortCode}' " +
            $"to reach click count {expectedCount}.");
    }
}