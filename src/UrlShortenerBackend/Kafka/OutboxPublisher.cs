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
    private const int DefaultBatchSize = 100;
    private const int DefaultPollingIntervalMs = 1000;
    private const int DefaultClaimTimeoutSeconds = 60;

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

        logger.LogInformation(
            "Outbox publisher started with batch size {BatchSize}, polling interval {PollingIntervalMs}ms and claim timeout {ClaimTimeoutSeconds}s",
            batchSize,
            pollingIntervalMs,
            claimTimeoutSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            var publishedAny =
                await PublishBatchAsync(
                    batchSize,
                    claimTimeoutSeconds,
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

        var publishedAny = false;

        foreach (var message in messages)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                break;
            }

            var published =
                await PublishMessageAsync(
                    context,
                    producer,
                    message,
                    cancellationToken);

            if (published)
            {
                publishedAny = true;
            }
        }

        await UpdateOutboxMetricsAsync(
            context,
            cancellationToken);

        return publishedAny;
    }

    private async Task<bool> PublishMessageAsync(
        UrlShortenerDbContext context,
        IClickEventProducer producer,
        OutboxMessage message,
        CancellationToken cancellationToken)
    {
        message.AttemptCount++;
        message.LastAttemptAt = DateTime.UtcNow;

        var stopwatch = Stopwatch.StartNew();

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

            message.PublishedAt = DateTime.UtcNow;
            message.ProcessingStartedAt = null;
            message.LastError = null;

            await context.SaveChangesAsync(
                cancellationToken);

            stopwatch.Stop();

            UrlShortenerMetrics.OutboxPublished.Add(1);

            UrlShortenerMetrics.OutboxPublishDuration.Record(
                stopwatch.Elapsed.TotalMilliseconds);

            logger.LogInformation(
                "Published outbox message {MessageId} for short code {ShortCode} on attempt {AttemptCount}",
                message.Id,
                clickEvent.ShortCode,
                message.AttemptCount);

            return true;
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            message.ProcessingStartedAt = null;
            message.LastError = ex.ToString();

            await context.SaveChangesAsync(
                cancellationToken);

            stopwatch.Stop();

            UrlShortenerMetrics.OutboxPublishFailures.Add(1);

            UrlShortenerMetrics.OutboxPublishDuration.Record(
                stopwatch.Elapsed.TotalMilliseconds);

            logger.LogWarning(
                ex,
                "Failed to publish outbox message {MessageId} on attempt {AttemptCount}. Message remains unpublished.",
                message.Id,
                message.AttemptCount);

            return false;
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
}