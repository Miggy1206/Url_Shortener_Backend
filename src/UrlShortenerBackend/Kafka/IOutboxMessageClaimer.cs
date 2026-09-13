using UrlShortenerBackend.Api.Models;

namespace UrlShortenerBackend.Api.Kafka;

public interface IOutboxMessageClaimer
{
    Task<List<OutboxMessage>> ClaimMessagesAsync(
    int batchSize,
    int claimTimeoutSeconds,
    CancellationToken cancellationToken = default);
}
