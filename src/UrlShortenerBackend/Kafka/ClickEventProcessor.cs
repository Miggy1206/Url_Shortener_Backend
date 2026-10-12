using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Kafka.Events;
using UrlShortenerBackend.Api.Models;
using UrlShortenerBackend.Api.Observability;

namespace UrlShortenerBackend.Api.Kafka;

public class ClickEventProcessor(
    IServiceScopeFactory scopeFactory,
    ILogger<ClickEventProcessor> logger) : IClickEventProcessor
{
    public async Task ProcessAsync(
        UrlClickedEvent clickEvent,
        CancellationToken cancellationToken = default)
    {
        await ProcessBatchAsync(
            [clickEvent],
            cancellationToken);
    }

    public async Task ProcessBatchAsync(
        IEnumerable<UrlClickedEvent> clickEvents,
        CancellationToken cancellationToken = default)
    {
        var events =
            clickEvents.ToList();

        if (events.Count == 0)
        {
            return;
        }

        using var activity =
            UrlShortenerActivitySource.Source.StartActivity(
                "click_event.process_batch",
                ActivityKind.Consumer);

        activity?.SetTag(
            "messaging.system",
            "kafka");

        activity?.SetTag(
            "messaging.destination.name",
            "url-clicked");

        activity?.SetTag(
            "urlshortener.batch_size",
            events.Count);

        var stopwatch =
            Stopwatch.StartNew();

        try
        {
            await using var scope =
                scopeFactory.CreateAsyncScope();

            var context =
                scope.ServiceProvider
                    .GetRequiredService<UrlShortenerDbContext>();

            await using var transaction =
                await context.Database.BeginTransactionAsync(
                    cancellationToken);

            /*
             * Identify duplicate events before modifying any click counts.
             * Existing ProcessedClickEvents rows are ignored.
             */
            var eventIds =
                events
                    .Select(x => x.EventId)
                    .Distinct()
                    .ToList();

            var alreadyProcessedIds =
                await context.ProcessedClickEvents
                    .Where(x => eventIds.Contains(x.EventId))
                    .Select(x => x.EventId)
                    .ToListAsync(
                        cancellationToken);

            var processedIdSet =
                alreadyProcessedIds.ToHashSet();

            var newEvents =
                events
                    .Where(x =>
                        !processedIdSet.Contains(x.EventId))
                    .GroupBy(x => x.EventId)
                    .Select(x => x.First())
                    .ToList();

            var duplicateCount =
                events.Count - newEvents.Count;

            if (duplicateCount > 0)
            {
                UrlShortenerMetrics.DuplicateClickEvents.Add(
                    duplicateCount);

                logger.LogDebug(
                    "Skipped {DuplicateCount} duplicate click events in batch",
                    duplicateCount);
            }

            if (newEvents.Count == 0)
            {
                await transaction.CommitAsync(
                    cancellationToken);

                return;
            }

            /*
             * Group events by short code so multiple clicks for the same URL
             * can be applied with a single SQL UPDATE per short code.
             */
            var eventsByShortCode =
                newEvents
                    .GroupBy(x => x.ShortCode)
                    .ToDictionary(
                        group => group.Key,
                        group => group.ToList());

            /*
             * Only events for existing URLs are considered successfully
             * processed. This preserves the original behaviour where an
             * unknown short code does not create a ProcessedClickEvent row.
             */
            var validEvents =
                new List<UrlClickedEvent>();

            foreach (var entry in eventsByShortCode)
            {
                var updatedRows =
                    await context.Urls
                        .Where(x =>
                            x.ShortCode == entry.Key)
                        .ExecuteUpdateAsync(
                            setters =>
                                setters.SetProperty(
                                    x => x.ClickCount,
                                    x =>
                                        x.ClickCount +
                                        entry.Value.Count),
                            cancellationToken);

                if (updatedRows == 0)
                {
                    logger.LogWarning(
                        "Batch contains click events referencing unknown short code {ShortCode}",
                        entry.Key);

                    continue;
                }

                validEvents.AddRange(
                    entry.Value);
            }

            /*
             * There may have been new events in the batch, but all of them
             * may have referenced unknown URLs. In that case there is nothing
             * to record in ProcessedClickEvents.
             */
            if (validEvents.Count == 0)
            {
                await transaction.CommitAsync(
                    cancellationToken);

                return;
            }

            /*
             * Persist idempotency records only for events that actually
             * updated an existing URL.
             */
            var processedAt =
                DateTime.UtcNow;

            foreach (var clickEvent in validEvents)
            {
                context.ProcessedClickEvents.Add(
                    new ProcessedClickEvent
                    {
                        EventId =
                            clickEvent.EventId,
                        ProcessedAt =
                            processedAt
                    });
            }

            await context.SaveChangesAsync(
                cancellationToken);

            await transaction.CommitAsync(
                cancellationToken);

            UrlShortenerMetrics.ClickEventsProcessed.Add(
                validEvents.Count);

            activity?.SetStatus(
                ActivityStatusCode.Ok);

            logger.LogDebug(
                "Successfully processed click event batch of {BatchSize} events with {ValidEventCount} valid events, {NewEventCount} new events and {DuplicateCount} duplicates",
                events.Count,
                validEvents.Count,
                newEvents.Count,
                duplicateCount);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        finally
        {
            stopwatch.Stop();

            UrlShortenerMetrics.ClickEventProcessingDuration.Record(
                stopwatch.Elapsed.TotalMilliseconds);
        }
    }
}