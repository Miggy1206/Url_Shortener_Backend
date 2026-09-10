using System.Diagnostics.Metrics;

namespace UrlShortenerBackend.Api.Observability;

public static class UrlShortenerMetrics
{
    public const string MeterName = "UrlShortenerBackend";

    public static readonly Meter Meter =
        new(MeterName);

    public static readonly Counter<long> ClickEventsPublished =
        Meter.CreateCounter<long>(
            "urlshortener.click_events.published",
            description: "Number of click events successfully published to Kafka.");

    public static readonly Counter<long> ClickEventPublishFailures =
        Meter.CreateCounter<long>(
            "urlshortener.click_events.publish_failures",
            description: "Number of click events that failed to publish to Kafka.");

    public static readonly Counter<long> ClickEventsProcessed =
        Meter.CreateCounter<long>(
            "urlshortener.click_events.processed",
            description: "Number of click events successfully processed.");

    public static readonly Counter<long> DuplicateClickEvents =
        Meter.CreateCounter<long>(
            "urlshortener.click_events.duplicates",
            description: "Number of duplicate click events skipped.");

    public static readonly Histogram<double> KafkaPublishDuration =
        Meter.CreateHistogram<double>(
            "urlshortener.kafka.publish.duration",
            unit: "ms",
            description: "Duration of Kafka click-event publishing.");

    public static readonly Histogram<double> ClickEventProcessingDuration =
        Meter.CreateHistogram<double>(
            "urlshortener.click_events.processing.duration",
            unit: "ms",
            description: "Duration of click-event processing.");
    
    public static readonly Counter<long> ClickEventPublishRetries =
        Meter.CreateCounter<long>(
            "urlshortener.click_events.publish_retries",
            description: "Number of click event publish retry attempts.");
    
    public static readonly Counter<long> KafkaCircuitOpened =
        Meter.CreateCounter<long>(
            "urlshortener.kafka.circuit.opened",
            description: "Number of times the Kafka circuit breaker opened.");

    public static readonly Counter<long> KafkaCircuitClosed =
        Meter.CreateCounter<long>(
            "urlshortener.kafka.circuit.closed",
            description: "Number of times the Kafka circuit breaker closed.");

    public static readonly Counter<long> KafkaCircuitHalfOpened =
        Meter.CreateCounter<long>(
            "urlshortener.kafka.circuit.half_opened",
            description: "Number of times the Kafka circuit breaker entered half-open state.");
}