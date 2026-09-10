using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Polly;
using Polly.CircuitBreaker;
using Polly.Registry;
using UrlShortenerBackend.Api.Kafka;
using UrlShortenerBackend.Api.Kafka.Events;

namespace UrlShortenerBackend.Tests.Kafka;

[Collection("Kafka tests")]
public class ClickEventProducerTests
{
    private static ResiliencePipelineProvider<string> CreatePipelineProvider()
    {
        var services = new ServiceCollection();

        services.AddResiliencePipeline(
            "kafka-publish",
            pipeline =>
            {
                pipeline.AddCircuitBreaker(
                    new CircuitBreakerStrategyOptions
                    {
                        FailureRatio = 0.5,
                        SamplingDuration = TimeSpan.FromSeconds(10),
                        MinimumThroughput = 5,
                        BreakDuration = TimeSpan.FromSeconds(30)
                    });
            });

        return services
            .BuildServiceProvider()
            .GetRequiredService<ResiliencePipelineProvider<string>>();
    }

    private static ResiliencePipelineProvider<string>
        CreateCircuitBreakerTestPipelineProvider()
    {
        var services = new ServiceCollection();

        services.AddResiliencePipeline(
            "kafka-publish",
            pipeline =>
            {
                pipeline.AddCircuitBreaker(
                    new CircuitBreakerStrategyOptions
                    {
                        FailureRatio = 0.5,
                        SamplingDuration = TimeSpan.FromSeconds(10),
                        MinimumThroughput = 2,
                        BreakDuration = TimeSpan.FromSeconds(5)
                    });
            });

        return services
            .BuildServiceProvider()
            .GetRequiredService<ResiliencePipelineProvider<string>>();
    }

    private static ClickEventProducer CreateProducer(
        IProducer<string, string> producer,
        ResiliencePipelineProvider<string>? pipelineProvider = null)
    {
        var configuration =
            new ConfigurationBuilder()
                .AddInMemoryCollection(
                    new Dictionary<string, string?>
                    {
                        ["Kafka:ClickTopic"] = "url-clicked"
                    })
                .Build();

        var provider =
            pipelineProvider ?? CreatePipelineProvider();

        return new ClickEventProducer(
            producer,
            configuration,
            Mock.Of<ILogger<ClickEventProducer>>(),
            provider);
    }

    private static UrlClickedEvent CreateClickEvent()
    {
        return new UrlClickedEvent(
            Guid.NewGuid(),
            "abc123",
            DateTime.UtcNow);
    }

    [Fact]
    public async Task PublishAsync_WhenFirstAttemptSucceeds_PublishesOnce()
    {
        // Arrange
        var producerMock =
            new Mock<IProducer<string, string>>();

        producerMock
            .Setup(x => x.ProduceAsync(
                "url-clicked",
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(
                new DeliveryResult<string, string>
                {
                    Topic = "url-clicked",
                    Partition = new Partition(0),
                    Offset = new Offset(1),
                    Message = new Message<string, string>()
                });

        var producer =
            CreateProducer(producerMock.Object);

        // Act
        await producer.PublishAsync(
            CreateClickEvent());

        // Assert
        producerMock.Verify(
            x => x.ProduceAsync(
                "url-clicked",
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task PublishAsync_WhenFirstAttemptFailsAndSecondSucceeds_Retries()
    {
        // Arrange
        var producerMock =
            new Mock<IProducer<string, string>>();

        var callCount = 0;

        producerMock
            .Setup(x => x.ProduceAsync(
                "url-clicked",
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()))
            .Returns<string, Message<string, string>, CancellationToken>(
                (_, _, _) =>
                {
                    callCount++;

                    if (callCount == 1)
                    {
                        return Task.FromException<
                            DeliveryResult<string, string>>(
                            new KafkaException(
                                new Error(
                                    ErrorCode.Local_TimedOut,
                                    "Kafka unavailable")));
                    }

                    return Task.FromResult(
                        new DeliveryResult<string, string>
                        {
                            Topic = "url-clicked",
                            Partition = new Partition(0),
                            Offset = new Offset(2),
                            Message = new Message<string, string>()
                        });
                });

        var producer =
            CreateProducer(producerMock.Object);

        // Act
        await producer.PublishAsync(
            CreateClickEvent());

        // Assert
        producerMock.Verify(
            x => x.ProduceAsync(
                "url-clicked",
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()),
            Times.Exactly(2));
    }

    [Fact]
    public async Task PublishAsync_WhenAllAttemptsFail_ThrowsKafkaException()
    {
        // Arrange
        var producerMock =
            new Mock<IProducer<string, string>>();

        producerMock
            .Setup(x => x.ProduceAsync(
                "url-clicked",
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(
                new KafkaException(
                    new Error(
                        ErrorCode.Local_TimedOut,
                        "Kafka unavailable")));

        var producer =
            CreateProducer(producerMock.Object);

        // Act / Assert
        await Assert.ThrowsAsync<KafkaException>(
            () => producer.PublishAsync(
                CreateClickEvent()));

        producerMock.Verify(
            x => x.ProduceAsync(
                "url-clicked",
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()),
            Times.Exactly(3));
    }

    [Fact]
    public async Task PublishAsync_WhenCircuitOpensDuringRetries_ThrowsBrokenCircuitException()
    {
        // Arrange
        var producerMock =
            new Mock<IProducer<string, string>>();

        producerMock
            .Setup(x => x.ProduceAsync(
                "url-clicked",
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(
                new KafkaException(
                    new Error(
                        ErrorCode.Local_TimedOut,
                        "Kafka unavailable")));

        var pipelineProvider =
            CreateCircuitBreakerTestPipelineProvider();

        var producer =
            CreateProducer(
                producerMock.Object,
                pipelineProvider);

        // Act
        await Assert.ThrowsAsync<BrokenCircuitException>(
            () => producer.PublishAsync(
                CreateClickEvent()));

        // The first two attempts reach Kafka.
        // The third attempt is blocked by the open circuit.
        // Assert
        producerMock.Verify(
            x => x.ProduceAsync(
                "url-clicked",
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()),
            Times.Exactly(2));
    }

    [Fact]
    public async Task PublishAsync_WhenCircuitCooldownExpires_AllowsRecovery()
    {
        // Arrange
        var producerMock =
            new Mock<IProducer<string, string>>();

        var callCount = 0;

        producerMock
            .Setup(x => x.ProduceAsync(
                "url-clicked",
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()))
            .Returns<string, Message<string, string>, CancellationToken>(
                (_, _, _) =>
                {
                    callCount++;

                    if (callCount <= 2)
                    {
                        return Task.FromException<
                            DeliveryResult<string, string>>(
                            new KafkaException(
                                new Error(
                                    ErrorCode.Local_TimedOut,
                                    "Kafka unavailable")));
                    }

                    return Task.FromResult(
                        new DeliveryResult<string, string>
                        {
                            Topic = "url-clicked",
                            Partition = new Partition(0),
                            Offset = new Offset(10),
                            Message = new Message<string, string>()
                        });
                });

        var pipelineProvider =
            CreateCircuitBreakerTestPipelineProvider();

        var producer =
            CreateProducer(
                producerMock.Object,
                pipelineProvider);

        // Act
        await Assert.ThrowsAsync<BrokenCircuitException>(
            () => producer.PublishAsync(
                CreateClickEvent()));

        // Circuit is now OPEN.
        await Assert.ThrowsAsync<BrokenCircuitException>(
            () => producer.PublishAsync(
                CreateClickEvent()));

        // Wait for the test break duration to expire.
        await Task.Delay(
            TimeSpan.FromSeconds(5.5));

        // Circuit should now be HALF-OPEN and allow a trial.
        await producer.PublishAsync(
            CreateClickEvent());

        // Assert
        producerMock.Verify(
            x => x.ProduceAsync(
                "url-clicked",
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()),
            Times.Exactly(3));
    }
}