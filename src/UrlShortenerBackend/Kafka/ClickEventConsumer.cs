using System.Text.Json;
using Confluent.Kafka;
using UrlShortenerBackend.Api.Kafka.Events;

namespace UrlShortenerBackend.Api.Kafka;

public class ClickEventConsumer(
    IConfiguration configuration,
    IServiceScopeFactory scopeFactory,
    ILogger<ClickEventConsumer> logger) : BackgroundService
{
    private readonly string _bootstrapServers =
        configuration["Kafka:BootstrapServers"]
        ?? throw new InvalidOperationException(
            "Kafka:BootstrapServers is not configured.");

    private readonly string _topic =
        configuration["Kafka:ClickTopic"]
        ?? throw new InvalidOperationException(
            "Kafka:ClickTopic is not configured.");

    private readonly string _groupId =
        configuration["Kafka:ConsumerGroup"]
        ?? throw new InvalidOperationException(
            "Kafka:ConsumerGroup is not configured.");

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = _groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer =
            new ConsumerBuilder<string, string>(
                consumerConfig)
            .Build();

        consumer.Subscribe(_topic);

        logger.LogInformation(
            "Kafka click event consumer started for topic {Topic}",
            _topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(
                        stoppingToken);

                    var clickEvent =
                        JsonSerializer.Deserialize<UrlClickedEvent>(
                            result.Message.Value);

                    if (clickEvent is null)
                    {
                        logger.LogWarning(
                            "Received invalid click event at partition {Partition} offset {Offset}",
                            result.Partition.Value,
                            result.Offset.Value);

                        consumer.Commit(result);
                        continue;
                    }

                    await using var scope =
                        scopeFactory.CreateAsyncScope();

                    var eventProcessor =
                        scope.ServiceProvider
                            .GetRequiredService<IClickEventProcessor>();

                    await eventProcessor.ProcessAsync(
                        clickEvent,
                        stoppingToken);

                    consumer.Commit(result);

                    logger.LogDebug(
                        "Processed click event {EventId} for short code {ShortCode} at partition {Partition} offset {Offset}",
                        clickEvent.EventId,
                        clickEvent.ShortCode,
                        result.Partition.Value,
                        result.Offset.Value);
                }
                catch (OperationCanceledException)
                    when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (ConsumeException ex)
                {
                    logger.LogError(
                        ex,
                        "Kafka consume error");
                }
                catch (JsonException ex)
                {
                    logger.LogError(
                        ex,
                        "Failed to deserialize Kafka click event");
                }
                catch (Exception ex)
                {
                    logger.LogError(
                        ex,
                        "Failed to process Kafka click event");
                }
            }
        }
        finally
        {
            consumer.Close();

            logger.LogInformation(
                "Kafka click event consumer stopped.");
        }
    }
}