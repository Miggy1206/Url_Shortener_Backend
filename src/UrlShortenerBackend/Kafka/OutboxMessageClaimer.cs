using Microsoft.EntityFrameworkCore;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Models;

namespace UrlShortenerBackend.Api.Kafka;

public sealed class OutboxMessageClaimer(
UrlShortenerDbContext context,
ILogger<OutboxMessageClaimer> logger) : IOutboxMessageClaimer
{
    public async Task<List<OutboxMessage>> ClaimMessagesAsync(
    int batchSize,
    int claimTimeoutSeconds,
    CancellationToken cancellationToken = default)
    {
        await using var transaction =
        await context.Database.BeginTransactionAsync(
        cancellationToken);

        var cutoff =
            DateTime.UtcNow.AddSeconds(
                -claimTimeoutSeconds);

        var messages =
            await context.OutboxMessages
                .FromSqlInterpolated(
                    $"""
                SELECT *
                FROM "OutboxMessages"
                WHERE "PublishedAt" IS NULL
                  AND (
                      "ProcessingStartedAt" IS NULL
                      OR "ProcessingStartedAt" < {cutoff}
                  )
                ORDER BY "OccurredAt"
                LIMIT {batchSize}
                FOR UPDATE SKIP LOCKED
                """)
                .ToListAsync(cancellationToken);

        if (messages.Count == 0)
        {
            await transaction.CommitAsync(
                cancellationToken);

            return [];
        }

        var now = DateTime.UtcNow;

        foreach (var message in messages)
        {
            message.ProcessingStartedAt = now;
        }

        await context.SaveChangesAsync(
            cancellationToken);

        await transaction.CommitAsync(
            cancellationToken);

        logger.LogDebug(
            "Claimed {MessageCount} outbox messages for publishing",
            messages.Count);

        return messages;
    }

}