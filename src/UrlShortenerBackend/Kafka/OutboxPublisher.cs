using System.Diagnostics;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Kafka.Events;
using UrlShortenerBackend.Api.Models;
using UrlShortenerBackend.Api.Observability;

namespace UrlShortenerBackend.Api.Kafka;

public sealed class OutboxPublisher(
    IServiceScopeFactory scopeFactory,
    IConfiguration configuration,
    ILogger<OutboxPublisher> logger) : BackgroundService
{
    private const int DefaultBatchSize = 500;
    private const int DefaultPollingIntervalMs = 1000;
    private const int DefaultClaimTimeoutSeconds = 60;
    private const int DefaultMaxConcurrentPublishes = 32;

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var batchSize =
            configuration.GetValue(
                "Outbox:BatchSize",
                DefaultBatchSize);

        var pollingIntervalMs =
            configuration.GetValue(
                "Outbox:PollingIntervalMs",
                DefaultPollingIntervalMs);

        var claimTimeoutSeconds =
            configuration.GetValue(
                "Outbox:ClaimTimeoutSeconds",
                DefaultClaimTimeoutSeconds);

        var maxConcurrentPublishes =
            configuration.GetValue(
                "Outbox:MaxConcurrentPublishes",
                DefaultMaxConcurrentPublishes);

        logger.LogInformation(
            "Outbox publisher started with batch size {BatchSize}, polling interval {PollingIntervalMs}ms, claim timeout {ClaimTimeoutSeconds}s and max concurrent publishes {MaxConcurrentPublishes}",
            batchSize,
            pollingIntervalMs,
            claimTimeoutSeconds,
            maxConcurrentPublishes);

        while (!stoppingToken.IsCancellationRequested)
        {
            var publishedAny =
                await PublishBatchAsync(
                    batchSize,
                    claimTimeoutSeconds,
                    maxConcurrentPublishes,
                    stoppingToken);

            if (!publishedAny)
            {
                await Task.Delay(
                    pollingIntervalMs,
                    stoppingToken);
            }
        }

        logger.LogInformation(
            "Outbox publisher stopped.");
    }

    private async Task<bool> PublishBatchAsync(
        int batchSize,
        int claimTimeoutSeconds,
        int maxConcurrentPublishes,
        CancellationToken cancellationToken)
    {
        await using var scope =
            scopeFactory.CreateAsyncScope();

        var context =
            scope.ServiceProvider
                .GetRequiredService<UrlShortenerDbContext>();

        var claimer =
            scope.ServiceProvider
                .GetRequiredService<IOutboxMessageClaimer>();

        var producer =
            scope.ServiceProvider
                .GetRequiredService<IClickEventProducer>();

        var messages =
            await claimer.ClaimMessagesAsync(
                batchSize,
                claimTimeoutSeconds,
                cancellationToken);

        await UpdateOutboxMetricsAsync(
            context,
            cancellationToken);

        if (messages.Count == 0)
        {
            return false;
        }

        var attemptStartedAt =
            DateTime.UtcNow;

        foreach (var message in messages)
        {
            message.AttemptCount++;
            message.LastAttemptAt = attemptStartedAt;
        }

        using var semaphore =
            new SemaphoreSlim(
                maxConcurrentPublishes,
                maxConcurrentPublishes);

        var tasks =
            messages.Select(
                message =>
                    PublishMessageAsync(
                        producer,
                        message,
                        semaphore,
                        cancellationToken));

        var results =
            await Task.WhenAll(tasks);

        foreach (var result in results)
        {
            var message = result.Message;

            if (result.Succeeded)
            {
                message.PublishedAt =
                    DateTime.UtcNow;

                message.ProcessingStartedAt =
                    null;

                message.LastError =
                    null;
            }
            else
            {
                message.ProcessingStartedAt =
                    null;

                message.LastError =
                    result.Error?.ToString();
            }
        }

        await context.SaveChangesAsync(
            cancellationToken);

        await UpdateOutboxMetricsAsync(
            context,
            cancellationToken);

        return results.Any(
            result => result.Succeeded);
    }

    private async Task<PublishResult> PublishMessageAsync(
        IClickEventProducer producer,
        OutboxMessage message,
        SemaphoreSlim semaphore,
        CancellationToken cancellationToken)
    {
        await semaphore.WaitAsync(
            cancellationToken);

        try
        {
            return await PublishToKafkaAsync(
                producer,
                message,
                cancellationToken);
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task<PublishResult> PublishToKafkaAsync(
        IClickEventProducer producer,
        OutboxMessage message,
        CancellationToken cancellationToken)
    {
        var stopwatch =
            Stopwatch.StartNew();

        try
        {
            if (message.Type != nameof(UrlClickedEvent))
            {
                throw new InvalidOperationException(
                    $"Unsupported outbox message type '{message.Type}'.");
            }

            var clickEvent =
                JsonSerializer.Deserialize<UrlClickedEvent>(
                    message.Payload);

            if (clickEvent is null)
            {
                throw new JsonException(
                    $"Unable to deserialize outbox message {message.Id}.");
            }

            await producer.PublishAsync(
                clickEvent,
                cancellationToken);

            stopwatch.Stop();

            UrlShortenerMetrics.OutboxPublished.Add(1);

            UrlShortenerMetrics.OutboxPublishDuration.Record(
                stopwatch.Elapsed.TotalMilliseconds);

            logger.LogDebug(
                "Published outbox message {MessageId} for short code {ShortCode} on attempt {AttemptCount}",
                message.Id,
                clickEvent.ShortCode,
                message.AttemptCount);

            return new PublishResult(
                message,
                true,
                null);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();

            UrlShortenerMetrics.OutboxPublishFailures.Add(1);

            UrlShortenerMetrics.OutboxPublishDuration.Record(
                stopwatch.Elapsed.TotalMilliseconds);

            logger.LogWarning(
                ex,
                "Failed to publish outbox message {MessageId} on attempt {AttemptCount}. Message remains unpublished.",
                message.Id,
                message.AttemptCount);

            return new PublishResult(
                message,
                false,
                ex);
        }
    }

    private static async Task UpdateOutboxMetricsAsync(
        UrlShortenerDbContext context,
        CancellationToken cancellationToken)
    {
        var oldestMessage =
            await context.OutboxMessages
                .AsNoTracking()
                .Where(x => x.PublishedAt == null)
                .OrderBy(x => x.OccurredAt)
                .Select(x => (DateTime?)x.OccurredAt)
                .FirstOrDefaultAsync(
                    cancellationToken);

        var backlog =
            await context.OutboxMessages
                .AsNoTracking()
                .CountAsync(
                    x => x.PublishedAt == null,
                    cancellationToken);

        UrlShortenerMetrics.SetOutboxBacklog(
            backlog,
            oldestMessage);
    }

    private sealed record PublishResult(
        OutboxMessage Message,
        bool Succeeded,
        Exception? Error);
}
