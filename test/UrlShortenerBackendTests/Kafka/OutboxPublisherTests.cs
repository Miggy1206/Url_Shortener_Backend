using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Kafka;
using UrlShortenerBackend.Api.Kafka.Events;
using UrlShortenerBackend.Api.Models;
using UrlShortenerBackend.Tests.Integration;

namespace UrlShortenerBackend.Tests.Kafka;

public class OutboxPublisherTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture _postgres;

    public OutboxPublisherTests(PostgresFixture postgres)
    {
        _postgres = postgres;
    }

    private DbContextOptions<UrlShortenerDbContext> CreateDbOptions()
    {
        return new DbContextOptionsBuilder<UrlShortenerDbContext>()
            .UseNpgsql(_postgres.ConnectionString)
            .Options;
    }

    private static UrlShortenerDbContext CreateDbContext(
        DbContextOptions<UrlShortenerDbContext> options)
    {
        return new UrlShortenerDbContext(options);
    }

    private UrlShortenerDbContext CreateDbContext()
    {
        return CreateDbContext(CreateDbOptions());
    }

    private async Task ClearDatabaseAsync()
    {
        await using var context = CreateDbContext();

        await context.Database.ExecuteSqlRawAsync(
            """
            TRUNCATE TABLE
                "OutboxMessages",
                "ProcessedClickEvents",
                "Urls"
            RESTART IDENTITY CASCADE
            """);
    }

    private static ServiceProvider CreateServiceProvider(
        DbContextOptions<UrlShortenerDbContext> dbOptions,
        IClickEventProducer producer)
    {
        var services = new ServiceCollection();

        services.AddScoped<UrlShortenerDbContext>(
            _ => new UrlShortenerDbContext(dbOptions));

        services.AddScoped<IClickEventProducer>(
            _ => producer);

        services.AddScoped<IOutboxMessageClaimer, OutboxMessageClaimer>();

        services.AddSingleton<IConfiguration>(
            new ConfigurationBuilder()
                .AddInMemoryCollection(
                    new Dictionary<string, string?>
                    {
                        ["Outbox:BatchSize"] = "10",
                        ["Outbox:PollingIntervalMs"] = "10",
                        ["Outbox:ClaimTimeoutSeconds"] = "60"
                    })
                .Build());

        services.AddLogging();

        services.AddSingleton<OutboxPublisher>();

        return services.BuildServiceProvider();
    }

    private static OutboxMessage CreateOutboxMessage(
        string shortCode = "abc123")
    {
        var clickEvent = new UrlClickedEvent(
            EventId: Guid.NewGuid(),
            ShortCode: shortCode,
            OccurredAt: DateTime.UtcNow);

        return new OutboxMessage
        {
            Id = clickEvent.EventId,
            Type = nameof(UrlClickedEvent),
            Payload = System.Text.Json.JsonSerializer.Serialize(
                clickEvent),
            OccurredAt = clickEvent.OccurredAt,
            PublishedAt = null,
            AttemptCount = 0,
            LastAttemptAt = null,
            LastError = null,
            ProcessingStartedAt = null
        };
    }

    private static async Task WaitForAsync(
        Func<Task<bool>> condition,
        TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow.Add(timeout);

        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(25);
        }

        Assert.Fail(
            $"Condition was not met within {timeout.TotalSeconds:F1} seconds.");
    }

    [Fact]
    public async Task Publisher_WhenMessagePublished_MarksMessageAsPublished()
    {
        // Arrange
        await ClearDatabaseAsync();

        var options = CreateDbOptions();

        await using (var arrangeContext = CreateDbContext(options))
        {
            arrangeContext.OutboxMessages.Add(
                CreateOutboxMessage());

            await arrangeContext.SaveChangesAsync();
        }

        var producerMock =
            new Mock<IClickEventProducer>();

        producerMock
            .Setup(x => x.PublishAsync(
                It.IsAny<UrlClickedEvent>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        await using var serviceProvider =
            CreateServiceProvider(
                options,
                producerMock.Object);

        var publisher =
            serviceProvider.GetRequiredService<OutboxPublisher>();

        // Act
        await publisher.StartAsync(
            CancellationToken.None);

        await WaitForAsync(
            async () =>
            {
                await using var context =
                    CreateDbContext(options);

                var message =
                    await context.OutboxMessages
                        .SingleAsync();

                return message.PublishedAt.HasValue;
            },
            TimeSpan.FromSeconds(5));

        using var stopCts =
            new CancellationTokenSource();

        await publisher.StopAsync(
            stopCts.Token);

        // Assert
        await using var assertContext =
            CreateDbContext(options);

        var publishedMessage =
            await assertContext.OutboxMessages
                .SingleAsync();

        Assert.NotNull(
            publishedMessage.PublishedAt);

        Assert.Equal(
            1,
            publishedMessage.AttemptCount);

        Assert.NotNull(
            publishedMessage.LastAttemptAt);

        Assert.Null(
            publishedMessage.LastError);

        Assert.Null(
            publishedMessage.ProcessingStartedAt);

        producerMock.Verify(
            x => x.PublishAsync(
                It.Is<UrlClickedEvent>(
                    e => e.ShortCode == "abc123"),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task Publisher_WhenPublishingFails_LeavesMessageUnpublished()
    {
        // Arrange
        await ClearDatabaseAsync();

        var options = CreateDbOptions();

        await using (var arrangeContext = CreateDbContext(options))
        {
            arrangeContext.OutboxMessages.Add(
                CreateOutboxMessage());

            await arrangeContext.SaveChangesAsync();
        }

        var producerMock =
            new Mock<IClickEventProducer>();

        producerMock
            .Setup(x => x.PublishAsync(
                It.IsAny<UrlClickedEvent>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(
                new InvalidOperationException(
                    "Kafka unavailable"));

        await using var serviceProvider =
            CreateServiceProvider(
                options,
                producerMock.Object);

        var publisher =
            serviceProvider.GetRequiredService<OutboxPublisher>();

        // Act
        await publisher.StartAsync(
            CancellationToken.None);

        await WaitForAsync(
            async () =>
            {
                await using var context =
                    CreateDbContext(options);

                var message =
                    await context.OutboxMessages
                        .SingleAsync();

                return message.AttemptCount >= 1;
            },
            TimeSpan.FromSeconds(5));

        using var stopCts =
            new CancellationTokenSource();

        await publisher.StopAsync(
            stopCts.Token);

        // Assert
        await using var assertContext =
            CreateDbContext(options);

        var failedMessage =
            await assertContext.OutboxMessages
                .SingleAsync();

        Assert.Null(
            failedMessage.PublishedAt);

        Assert.True(
            failedMessage.AttemptCount >= 1);

        Assert.NotNull(
            failedMessage.LastAttemptAt);

        Assert.Contains(
            "Kafka unavailable",
            failedMessage.LastError);

        Assert.Null(
            failedMessage.ProcessingStartedAt);

        producerMock.Verify(
            x => x.PublishAsync(
                It.IsAny<UrlClickedEvent>(),
                It.IsAny<CancellationToken>()),
            Times.AtLeastOnce);
    }

    [Fact]
    public async Task Publisher_WhenPublishingFailsThenSucceeds_EventuallyMarksMessageAsPublished()
    {
        // Arrange
        await ClearDatabaseAsync();

        var options = CreateDbOptions();

        await using (var arrangeContext = CreateDbContext(options))
        {
            arrangeContext.OutboxMessages.Add(
                CreateOutboxMessage());

            await arrangeContext.SaveChangesAsync();
        }

        var producerMock =
            new Mock<IClickEventProducer>();

        var attempts = 0;

        producerMock
            .Setup(x => x.PublishAsync(
                It.IsAny<UrlClickedEvent>(),
                It.IsAny<CancellationToken>()))
            .Returns(
                (
                    UrlClickedEvent _,
                    CancellationToken _) =>
                {
                    attempts++;

                    if (attempts == 1)
                    {
                        return Task.FromException(
                            new InvalidOperationException(
                                "Kafka temporarily unavailable"));
                    }

                    return Task.CompletedTask;
                });

        await using var serviceProvider =
            CreateServiceProvider(
                options,
                producerMock.Object);

        var publisher =
            serviceProvider.GetRequiredService<OutboxPublisher>();

        // Act
        await publisher.StartAsync(
            CancellationToken.None);

        await WaitForAsync(
            async () =>
            {
                await using var context =
                    CreateDbContext(options);

                var message =
                    await context.OutboxMessages
                        .SingleAsync();

                return message.PublishedAt.HasValue;
            },
            TimeSpan.FromSeconds(5));

        using var stopCts =
            new CancellationTokenSource();

        await publisher.StopAsync(
            stopCts.Token);

        // Assert
        await using var assertContext =
            CreateDbContext(options);

        var publishedMessage =
            await assertContext.OutboxMessages
                .SingleAsync();

        Assert.NotNull(
            publishedMessage.PublishedAt);

        Assert.Equal(
            2,
            publishedMessage.AttemptCount);

        Assert.Equal(
            2,
            attempts);

        Assert.Null(
            publishedMessage.LastError);

        Assert.Null(
            publishedMessage.ProcessingStartedAt);
    }

    [Fact]
    public async Task Publisher_WhenMessageAlreadyPublished_DoesNotPublishAgain()
    {
        // Arrange
        await ClearDatabaseAsync();

        var options = CreateDbOptions();

        var message = CreateOutboxMessage();

        message.PublishedAt = DateTime.UtcNow;
        message.AttemptCount = 1;
        message.LastAttemptAt = DateTime.UtcNow;

        await using (var arrangeContext = CreateDbContext(options))
        {
            arrangeContext.OutboxMessages.Add(message);

            await arrangeContext.SaveChangesAsync();
        }

        var producerMock =
            new Mock<IClickEventProducer>();

        await using var serviceProvider =
            CreateServiceProvider(
                options,
                producerMock.Object);

        var publisher =
            serviceProvider.GetRequiredService<OutboxPublisher>();

        // Act
        await publisher.StartAsync(
            CancellationToken.None);

        await Task.Delay(100);

        using var stopCts =
            new CancellationTokenSource();

        await publisher.StopAsync(
            stopCts.Token);

        // Assert
        producerMock.Verify(
            x => x.PublishAsync(
                It.IsAny<UrlClickedEvent>(),
                It.IsAny<CancellationToken>()),
            Times.Never);

        await using var assertContext =
            CreateDbContext(options);

        var persistedMessage =
            await assertContext.OutboxMessages
                .SingleAsync();

        Assert.NotNull(
            persistedMessage.PublishedAt);

        Assert.Equal(
            1,
            persistedMessage.AttemptCount);
    }

    [Fact]
    public async Task Publisher_PublishesMultipleMessages()
    {
        // Arrange
        await ClearDatabaseAsync();

        var options = CreateDbOptions();

        await using (var arrangeContext = CreateDbContext(options))
        {
            arrangeContext.OutboxMessages.AddRange(
                CreateOutboxMessage("abc123"),
                CreateOutboxMessage("def456"),
                CreateOutboxMessage("ghi789"));

            await arrangeContext.SaveChangesAsync();
        }

        var producerMock =
            new Mock<IClickEventProducer>();

        producerMock
            .Setup(x => x.PublishAsync(
                It.IsAny<UrlClickedEvent>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        await using var serviceProvider =
            CreateServiceProvider(
                options,
                producerMock.Object);

        var publisher =
            serviceProvider.GetRequiredService<OutboxPublisher>();

        // Act
        await publisher.StartAsync(
            CancellationToken.None);

        await WaitForAsync(
            async () =>
            {
                await using var context =
                    CreateDbContext(options);

                return await context.OutboxMessages
                    .AllAsync(
                        x => x.PublishedAt.HasValue);
            },
            TimeSpan.FromSeconds(5));

        using var stopCts =
            new CancellationTokenSource();

        await publisher.StopAsync(
            stopCts.Token);

        // Assert
        await using var assertContext =
            CreateDbContext(options);

        var messages =
            await assertContext.OutboxMessages
                .OrderBy(x => x.OccurredAt)
                .ToListAsync();

        Assert.Equal(
            3,
            messages.Count);

        Assert.All(
            messages,
            message =>
            {
                Assert.NotNull(
                    message.PublishedAt);

                Assert.Equal(
                    1,
                    message.AttemptCount);

                Assert.Null(
                    message.ProcessingStartedAt);
            });

        producerMock.Verify(
            x => x.PublishAsync(
                It.IsAny<UrlClickedEvent>(),
                It.IsAny<CancellationToken>()),
            Times.Exactly(3));
    }

    [Fact]
    public async Task ClaimMessages_WhenAnotherTransactionHoldsLock_SkipsLockedMessage()
    {
        // Arrange
        await ClearDatabaseAsync();

        var options = CreateDbOptions();

        await using (var arrangeContext = CreateDbContext(options))
        {
            arrangeContext.OutboxMessages.Add(
                CreateOutboxMessage("abc123"));

            await arrangeContext.SaveChangesAsync();
        }

        await using var firstContext =
            CreateDbContext(options);

        await using var secondContext =
            CreateDbContext(options);

        await using var firstTransaction =
            await firstContext.Database.BeginTransactionAsync();

        var firstClaim =
            await firstContext.OutboxMessages
                .FromSqlRaw(
                    """
            SELECT *
            FROM "OutboxMessages"
            WHERE "PublishedAt" IS NULL
            ORDER BY "OccurredAt"
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """)
                .ToListAsync();

        // The first transaction has now locked the message.
        Assert.Single(firstClaim);

        // Act
        await using var secondTransaction =
            await secondContext.Database.BeginTransactionAsync();

        var secondClaim =
            await secondContext.OutboxMessages
                .FromSqlRaw(
                    """
            SELECT *
            FROM "OutboxMessages"
            WHERE "PublishedAt" IS NULL
            ORDER BY "OccurredAt"
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """)
                .ToListAsync();

        // Assert
        Assert.Empty(secondClaim);

        await secondTransaction.CommitAsync();
        await firstTransaction.RollbackAsync();

    }


    [Fact]
    public async Task Publisher_ReclaimsStaleClaim()
    {
        // Arrange
        await ClearDatabaseAsync();

        var options = CreateDbOptions();

        var message = CreateOutboxMessage("abc123");

        message.ProcessingStartedAt =
            DateTime.UtcNow.AddMinutes(-5);

        await using (var arrangeContext = CreateDbContext(options))
        {
            arrangeContext.OutboxMessages.Add(message);

            await arrangeContext.SaveChangesAsync();
        }

        var producerMock =
            new Mock<IClickEventProducer>();

        producerMock
            .Setup(x => x.PublishAsync(
                It.IsAny<UrlClickedEvent>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        await using var serviceProvider =
            CreateServiceProvider(
                options,
                producerMock.Object);

        var publisher =
            serviceProvider.GetRequiredService<OutboxPublisher>();

        // Act
        await publisher.StartAsync(
            CancellationToken.None);

        await WaitForAsync(
            async () =>
            {
                await using var context =
                    CreateDbContext(options);

                var persistedMessage =
                    await context.OutboxMessages
                        .SingleAsync();

                return persistedMessage.PublishedAt.HasValue;
            },
            TimeSpan.FromSeconds(5));

        using var stopCts =
            new CancellationTokenSource();

        await publisher.StopAsync(
            stopCts.Token);

        // Assert
        await using var assertContext =
            CreateDbContext(options);

        var publishedMessage =
            await assertContext.OutboxMessages
                .SingleAsync();

        Assert.NotNull(
            publishedMessage.PublishedAt);

        Assert.Null(
            publishedMessage.ProcessingStartedAt);

        Assert.Equal(
            1,
            publishedMessage.AttemptCount);

        producerMock.Verify(
            x => x.PublishAsync(
                It.Is<UrlClickedEvent>(
                    e => e.ShortCode == "abc123"),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }
}