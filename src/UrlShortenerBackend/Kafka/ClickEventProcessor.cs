using Microsoft.EntityFrameworkCore;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Kafka.Events;
using UrlShortenerBackend.Api.Models;

namespace UrlShortenerBackend.Api.Kafka;

public class ClickEventProcessor(
    IServiceScopeFactory scopeFactory,
    ILogger<ClickEventProcessor> logger) : IClickEventProcessor
{
    public async Task ProcessAsync(
        UrlClickedEvent clickEvent,
        CancellationToken cancellationToken = default)
    {
        await using var scope =
            scopeFactory.CreateAsyncScope();

        var context =
            scope.ServiceProvider
                .GetRequiredService<UrlShortenerDbContext>();

        await using var transaction =
            await context.Database.BeginTransactionAsync(
                cancellationToken);

        var alreadyProcessed =
            await context.ProcessedClickEvents
                .AnyAsync(
                    x => x.EventId == clickEvent.EventId,
                    cancellationToken);

        if (alreadyProcessed)
        {
            logger.LogDebug(
                "Skipping already processed click event {EventId}",
                clickEvent.EventId);

            await transaction.CommitAsync(
                cancellationToken);

            return;
        }

        var updatedRows =
            await context.Urls
                .Where(x => x.ShortCode == clickEvent.ShortCode)
                .ExecuteUpdateAsync(
                    setters => setters.SetProperty(
                        x => x.ClickCount,
                        x => x.ClickCount + 1),
                    cancellationToken);

        if (updatedRows == 0)
        {
            logger.LogWarning(
                "Click event {EventId} references unknown short code {ShortCode}",
                clickEvent.EventId,
                clickEvent.ShortCode);

            await transaction.RollbackAsync(
                cancellationToken);

            return;
        }

        context.ProcessedClickEvents.Add(
            new ProcessedClickEvent
            {
                EventId = clickEvent.EventId,
                ProcessedAt = DateTime.UtcNow
            });

        await context.SaveChangesAsync(
            cancellationToken);

        await transaction.CommitAsync(
            cancellationToken);
    }
}