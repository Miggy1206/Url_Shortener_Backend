using Confluent.Kafka;
using System.Text.Json;
using UrlShortenerBackend.Api.Kafka.Events;
using UrlShortenerBackend.Api.Observability;
using System.Diagnostics;
using OpenTelemetry.Context.Propagation;

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
        UrlClickedEvent clickEvent,
        CancellationToken cancellationToken = default)
    {
        using var activity =
            UrlShortenerActivitySource.Source.StartActivity(
                "kafka.publish",
                ActivityKind.Producer);

        activity?.SetTag(
            "messaging.system",
            "kafka");

        activity?.SetTag(
            "messaging.destination.name",
            _topic);

        activity?.SetTag(
            "urlshortener.event_id",
            clickEvent.EventId);

        var message = new Message<string, string>
        {
            Key = clickEvent.ShortCode,
            Value = JsonSerializer.Serialize(clickEvent)
        };

        Propagators.DefaultTextMapPropagator.Inject(
            new PropagationContext(
                Activity.Current?.Context ?? default,
                default),
            message,
            static (carrier, key, value) =>
            {
                carrier.Headers ??= new Headers();

                carrier.Headers.Add(
                    key,
                    System.Text.Encoding.UTF8.GetBytes(value));
            });

        var stopwatch = Stopwatch.StartNew();

        try
        {
            var result = await producer.ProduceAsync(
                _topic,
                message,
                cancellationToken);

            stopwatch.Stop();

            UrlShortenerMetrics.KafkaPublishDuration.Record(
                stopwatch.Elapsed.TotalMilliseconds);

            UrlShortenerMetrics.ClickEventsPublished.Add(1);

            logger.LogDebug(
                "Published click event {EventId} for short code {ShortCode} to Kafka partition {Partition} at offset {Offset}",
                clickEvent.EventId,
                clickEvent.ShortCode,
                result.Partition.Value,
                result.Offset.Value);
            
            activity?.SetTag(
                "messaging.kafka.partition",
                result.Partition.Value);
            
            activity?.SetTag(
                "messaging.kafka.offset",
                result.Offset.Value);
        }
        catch(Exception ex)
        {
            activity?.SetStatus(
                ActivityStatusCode.Error);

            activity?.SetTag("error.type", ex.GetType().FullName);
            activity?.SetTag("error.message", ex.Message);
            activity?.SetStatus(
                ActivityStatusCode.Error,
                ex.Message);
            
            stopwatch.Stop();

            UrlShortenerMetrics.KafkaPublishDuration.Record(
                stopwatch.Elapsed.TotalMilliseconds);

            UrlShortenerMetrics.ClickEventPublishFailures.Add(1);

            throw;
        }
    }
      
}