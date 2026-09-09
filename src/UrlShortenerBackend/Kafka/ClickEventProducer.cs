using Confluent.Kafka;
using System.Text.Json;
using UrlShortenerBackend.Api.Kafka.Events;

namespace UrlShortenerBackend.Api.Kafka;

public class ClickEventProducer(
    IProducer<string,string> producer,
    IConfiguration configuration,
    ILogger<ClickEventProducer> logger) : IClickEventProducer
{
    private readonly string _topic =
        configuration["Kafka:ClickTopic"]
        ?? throw new InvalidOperationException("Kafka:ClickTopic configuration is missing");
    
    public async Task PublishAsync(
        UrlClickedEvent urlClickedEvent,
        CancellationToken cancellationToken = default)
    {
        var message = new Message<string, string>
        {
            Key = urlClickedEvent.ShortCode,
            Value = JsonSerializer.Serialize(urlClickedEvent)
        };

        var result = await producer.ProduceAsync(
            _topic,
            message,
            cancellationToken
        );

        logger.LogDebug(
            "Published click event {EventId} for shortcode {ShortCode} to kafka partition {Partition} at offset {Offset}",
            urlClickedEvent.EventId,
            urlClickedEvent.ShortCode,
            result.Partition.Value,
            result.Offset.Value
        );
    }
      
}