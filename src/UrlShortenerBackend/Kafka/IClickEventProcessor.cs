using UrlShortenerBackend.Api.Kafka.Events;

namespace UrlShortenerBackend.Api.Kafka;

public interface IClickEventProcessor
{
    Task ProcessAsync(
        UrlClickedEvent clickEvent,
        CancellationToken cancellationToken = default);
}