using System.Diagnostics.Metrics;

namespace UrlShortenerBackend.Api.Observability;

public static class UrlShortenerMetrics
{
    public const string MeterName = "UrlShortenerBackend";

    private static readonly Meter Meter =
        new(MeterName);

    private static long _outboxBacklog;
    private static long _outboxOldestMessageTicks;

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

    public static readonly ObservableGauge<long> OutboxBacklog =
        Meter.CreateObservableGauge(
            "urlshortener.outbox.backlog",
            () => Volatile.Read(ref _outboxBacklog),
            unit: "{messages}",
            description: "Number of unpublished outbox messages.");

    public static readonly ObservableGauge<double> OutboxOldestAge =
        Meter.CreateObservableGauge(
            "urlshortener.outbox.oldest_age",
            () =>
            {
                var ticks =
                    Volatile.Read(ref _outboxOldestMessageTicks);

                if (ticks == 0)
                {
                    return 0;
                }

                var occurredAt =
                    new DateTime(
                        ticks,
                        DateTimeKind.Utc);

                return Math.Max(
                    0,
                    (DateTime.UtcNow - occurredAt).TotalSeconds);
            },
            unit: "s",
            description: "Age of the oldest unpublished outbox message.");

    public static readonly Counter<long> OutboxPublished =
        Meter.CreateCounter<long>(
            "urlshortener.outbox.published",
            unit: "{messages}",
            description: "Number of outbox messages successfully published.");

    public static readonly Counter<long> OutboxPublishFailures =
        Meter.CreateCounter<long>(
            "urlshortener.outbox.publish_failures",
            unit: "{messages}",
            description: "Number of failed outbox publish attempts.");

    public static readonly Histogram<double> OutboxPublishDuration =
        Meter.CreateHistogram<double>(
            "urlshortener.outbox.publish.duration",
            unit: "ms",
            description: "Time spent publishing an outbox message.");

    public static void SetOutboxBacklog(
        long backlog,
        DateTime? oldestOccurredAt)
    {
        Interlocked.Exchange(
            ref _outboxBacklog,
            backlog);

        Interlocked.Exchange(
            ref _outboxOldestMessageTicks,
            oldestOccurredAt?.Ticks ?? 0);
    }
}