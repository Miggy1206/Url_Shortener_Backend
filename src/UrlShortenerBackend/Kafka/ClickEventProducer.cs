using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using OpenTelemetry.Context.Propagation;
using UrlShortenerBackend.Api.Kafka.Events;
using UrlShortenerBackend.Api.Observability;
using Polly;
using Polly.Registry;

namespace UrlShortenerBackend.Api.Kafka;

public class ClickEventProducer(
    IProducer<string, string> producer,
    IConfiguration configuration,
    ILogger<ClickEventProducer> logger,
    ResiliencePipelineProvider<string> pipelineProvider) : IClickEventProducer
{

    private readonly ResiliencePipeline _pipeline =
        pipelineProvider.GetPipeline("kafka-publish");

    private readonly string _topic =
        configuration["Kafka:ClickTopic"]
        ?? throw new InvalidOperationException(
            "Kafka:ClickTopic configuration is missing");

    private readonly int _maxAttempts =
        configuration.GetValue(
            "Kafka:Retry:MaxAttempts",
            3);

    private readonly int _initialDelayMs =
        configuration.GetValue(
            "Kafka:Retry:InitialDelayMs",
            50);

    private readonly int _maxDelayMs =
        configuration.GetValue(
            "Kafka:Retry:MaxDelayMs",
            100);

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

        for (var attempt = 1; attempt <= _maxAttempts; attempt++)
        {
            var stopwatch = Stopwatch.StartNew();

            try
            {
                var result = await _pipeline.ExecuteAsync(
                    async token =>
                        await producer.ProduceAsync(
                            _topic,
                            message,
                            token),
                    cancellationToken);

                stopwatch.Stop();

                UrlShortenerMetrics.KafkaPublishDuration.Record(
                    stopwatch.Elapsed.TotalMilliseconds);

                UrlShortenerMetrics.ClickEventsPublished.Add(1);

                activity?.SetTag(
                    "messaging.kafka.partition",
                    result.Partition.Value);

                activity?.SetTag(
                    "messaging.kafka.offset",
                    result.Offset.Value);

                logger.LogDebug(
                    "Published click event {EventId} for short code {ShortCode} to Kafka partition {Partition} at offset {Offset} on attempt {Attempt}",
                    clickEvent.EventId,
                    clickEvent.ShortCode,
                    result.Partition.Value,
                    result.Offset.Value,
                    attempt);

                return;
            }
            catch (Exception ex) when (
                attempt < _maxAttempts &&
                !cancellationToken.IsCancellationRequested)
            {
                stopwatch.Stop();

                UrlShortenerMetrics.KafkaPublishDuration.Record(
                    stopwatch.Elapsed.TotalMilliseconds);

                UrlShortenerMetrics.ClickEventPublishRetries.Add(1);

                var delayMs = Math.Min(
                    _initialDelayMs * (int)Math.Pow(2, attempt - 1),
                    _maxDelayMs);

                logger.LogWarning(
                    ex,
                    "Kafka publish attempt {Attempt} failed for click event {EventId}. Retrying after {DelayMs}ms.",
                    attempt,
                    clickEvent.EventId,
                    delayMs);

                await Task.Delay(
                    TimeSpan.FromMilliseconds(delayMs),
                    cancellationToken);
            }
            catch (Exception ex)
            {
                stopwatch.Stop();

                UrlShortenerMetrics.KafkaPublishDuration.Record(
                    stopwatch.Elapsed.TotalMilliseconds);

                UrlShortenerMetrics.ClickEventPublishFailures.Add(1);

                activity?.SetTag(
                    "error.type",
                    ex.GetType().FullName);

                activity?.SetTag(
                    "error.message",
                    ex.Message);

                activity?.SetStatus(
                    ActivityStatusCode.Error,
                    ex.Message);

                logger.LogError(
                    ex,
                    "Failed to publish click event {EventId} for short code {ShortCode} after {Attempts} attempts.",
                    clickEvent.EventId,
                    clickEvent.ShortCode,
                    attempt);

                throw;
            }
        }
    }
}