using UrlShortenerBackend.Api.Kafka.Events;

namespace UrlShortenerBackend.Api.Kafka;

public interface IClickEventProducer
{
    Task PublishAsync(
        UrlClickedEvent urlClickedEvent,
        CancellationToken cancellationToken = default
    );
}