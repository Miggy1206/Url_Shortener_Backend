namespace UrlShortenerBackend.Api.Kafka.Events;

public record UrlClickedEvent(
    Guid EventId,
    string ShortCode,
    DateTime OccurredAt
);