using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Kafka;
using UrlShortenerBackend.Api.Kafka.Events;
using UrlShortenerBackend.Api.Models;
using UrlShortenerBackend.Tests.Integration;

namespace UrlShortenerBackend.Tests.Kafka;

public class ClickEventProcessorTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture _postgres;

    public ClickEventProcessorTests(PostgresFixture postgres)
    {
        _postgres = postgres;
    }

    private ServiceProvider CreateServiceProvider()
    {
        var services = new ServiceCollection();

        services.AddDbContext<UrlShortenerDbContext>(
            options => options.UseNpgsql(
                _postgres.ConnectionString));

        services.AddLogging();

        services.AddScoped<
            ClickEventProcessor>();

        return services.BuildServiceProvider();
    }

    private async Task AddUrlAsync(
        string shortCode,
        int clickCount = 0)
    {
        await using var context = CreateDbContext();

        context.Urls.Add(
            new Url
            {
                OriginalUrl = "https://www.example.com",
                ShortCode = shortCode,
                CreatedAt = DateTime.UtcNow,
                ClickCount = clickCount
            });

        await context.SaveChangesAsync();
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
    public async Task ProcessAsync_WithNewEvent_IncrementsClickCount()
    {
        // Arrange
        var shortCode = $"test{Guid.NewGuid():N}"[..6];

        await AddUrlAsync(shortCode);

        var eventId = Guid.NewGuid();

        var clickEvent = new UrlClickedEvent(
            eventId,
            shortCode,
            DateTime.UtcNow);

        await using var provider =
            CreateServiceProvider();

        var processor =
            provider.GetRequiredService<ClickEventProcessor>();

        // Act
        await processor.ProcessAsync(clickEvent);

        // Assert
        await using var verificationContext =
            CreateDbContext();

        var url = await verificationContext.Urls
            .SingleAsync(x => x.ShortCode == shortCode);

        Assert.Equal(1, url.ClickCount);

        var processedEvent =
            await verificationContext.ProcessedClickEvents
                .SingleAsync(x => x.EventId == eventId);

        Assert.Equal(
            eventId,
            processedEvent.EventId);
    }

    [Fact]
    public async Task ProcessAsync_WithDuplicateEvent_DoesNotIncrementClickCountTwice()
    {
        // Arrange
        var shortCode = $"test{Guid.NewGuid():N}"[..6];

        await AddUrlAsync(shortCode);

        var eventId = Guid.NewGuid();

        var clickEvent = new UrlClickedEvent(
            eventId,
            shortCode,
            DateTime.UtcNow);

        await using var provider =
            CreateServiceProvider();

        var processor =
            provider.GetRequiredService<ClickEventProcessor>();

        // Act
        await processor.ProcessAsync(clickEvent);
        await processor.ProcessAsync(clickEvent);

        // Assert
        await using var verificationContext =
            CreateDbContext();

        var url = await verificationContext.Urls
            .SingleAsync(x => x.ShortCode == shortCode);

        Assert.Equal(1, url.ClickCount);

        var processedEvents =
            await verificationContext.ProcessedClickEvents
                .Where(x => x.EventId == eventId)
                .ToListAsync();

        Assert.Single(processedEvents);
    }

    [Fact]
    public async Task ProcessAsync_WithUnknownShortCode_DoesNotRecordEvent()
    {
        // Arrange
        var eventId = Guid.NewGuid();

        var clickEvent = new UrlClickedEvent(
            eventId,
            $"missing{Guid.NewGuid():N}"[..6],
            DateTime.UtcNow);

        await using var provider =
            CreateServiceProvider();

        var processor =
            provider.GetRequiredService<ClickEventProcessor>();

        // Act
        await processor.ProcessAsync(clickEvent);

        // Assert
        await using var verificationContext =
            CreateDbContext();

        var processedEvent =
            await verificationContext.ProcessedClickEvents
                .SingleOrDefaultAsync(
                    x => x.EventId == eventId);

        Assert.Null(processedEvent);
    }
}