using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using StackExchange.Redis;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Models;
using UrlShortenerBackend.Api.Services;
using UrlShortenerBackend.Tests.Integration;

namespace UrlShortenerBackend.Tests.Services;

public class UrlShortenerServiceTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture _postgres;

    public UrlShortenerServiceTests(PostgresFixture postgres)
    {
        _postgres = postgres;
    }

    private UrlShortenerDbContext CreateDbContext()
    {
        var options = new DbContextOptionsBuilder<UrlShortenerDbContext>()
            .UseNpgsql(_postgres.ConnectionString)
            .Options;

        return new UrlShortenerDbContext(options);
    }

    private static IConnectionMultiplexer CreateRedisMock()
    {
        var redisMock = new Mock<IConnectionMultiplexer>();
        var databaseMock = new Mock<IDatabase>();

        redisMock
            .Setup(x => x.GetDatabase(
                It.IsAny<int>(),
                It.IsAny<object?>()))
            .Returns(databaseMock.Object);

        databaseMock
            .Setup(x => x.StringGetAsync(
                It.IsAny<RedisKey>(),
                It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisValue.Null);

        return redisMock.Object;
    }

    private static UrlShortenerService CreateService(
        UrlShortenerDbContext context,
        IConnectionMultiplexer redis,
        ILogger<UrlShortenerService>? logger = null)
    {
        return new UrlShortenerService(
            context,
            redis,
            logger ?? NullLogger<UrlShortenerService>.Instance);
    }

    private async Task ClearUrlsAsync()
    {
        await using var context = CreateDbContext();

        await context.Database.ExecuteSqlRawAsync(
            "TRUNCATE TABLE \"Urls\" RESTART IDENTITY CASCADE");
    }

    [Fact]
    public async Task CreateShortUrlAsync_WithValidUrl_CreatesUrl()
    {
        // Arrange
        await ClearUrlsAsync();

        await using var context = CreateDbContext();

        var service = CreateService(
            context,
            CreateRedisMock());

        // Act
        var result = await service.CreateShortUrlAsync(
            "https://www.example.com");

        // Assert
        Assert.Equal(
            "https://www.example.com",
            result.OriginalUrl);

        Assert.Equal(6, result.ShortCode.Length);
        Assert.Equal(0, result.ClickCount);
        Assert.NotEqual(default, result.CreatedAt);

        Assert.Single(context.Urls);
    }

    [Fact]
    public async Task CreateShortUrlAsync_GeneratesUniqueShortCodes()
    {
        // Arrange
        await ClearUrlsAsync();

        await using var context = CreateDbContext();

        var service = CreateService(
            context,
            CreateRedisMock());

        // Act
        var first = await service.CreateShortUrlAsync(
            "https://www.example.com/1");

        var second = await service.CreateShortUrlAsync(
            "https://www.example.com/2");

        // Assert
        Assert.NotEqual(
            first.ShortCode,
            second.ShortCode);
    }

    [Fact]
    public async Task RedirectUrlAsync_WithExistingShortCode_ReturnsUrl()
    {
        // Arrange
        await ClearUrlsAsync();

        await using var context = CreateDbContext();

        var created = await serviceCreateUrlAsync(
            context);

        var service = CreateService(
            context,
            CreateRedisMock());

        // Act
        var result = await service.RedirectUrlAsync(
            created.ShortCode);

        // Assert
        Assert.Equal(
            "https://www.example.com",
            result);
    }

    [Fact]
    public async Task RedirectUrlAsync_WithExistingShortCode_IncrementsClickCount()
    {
        // Arrange
        await ClearUrlsAsync();

        await using var context = CreateDbContext();

        var created = await serviceCreateUrlAsync(
            context);

        var service = CreateService(
            context,
            CreateRedisMock());

        // Act
        await service.RedirectUrlAsync(
            created.ShortCode);

        // Assert
        await using var verificationContext = CreateDbContext();

        var savedUrl = await verificationContext.Urls
            .SingleAsync(x => x.ShortCode == created.ShortCode);

        Assert.Equal(1, savedUrl.ClickCount);
    }

    [Fact]
    public async Task RedirectUrlAsync_WithUnknownShortCode_ReturnsNull()
    {
        // Arrange
        await ClearUrlsAsync();

        await using var context = CreateDbContext();

        var service = CreateService(
            context,
            CreateRedisMock());

        // Act
        var result = await service.RedirectUrlAsync("missing");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task RedirectUrl_WithCachedUrl_ReturnsCachedUrl()
    {
        // Arrange
        await ClearUrlsAsync();

        await using var context = CreateDbContext();

        context.Urls.Add(new Url
        {
            OriginalUrl = "https://www.example.com",
            ShortCode = "abc123",
            CreatedAt = DateTime.UtcNow,
            ClickCount = 0
        });

        await context.SaveChangesAsync();

        var redisMock = new Mock<IConnectionMultiplexer>();
        var databaseMock = new Mock<IDatabase>();

        redisMock
            .Setup(x => x.GetDatabase(
                It.IsAny<int>(),
                It.IsAny<object?>()))
            .Returns(databaseMock.Object);

        databaseMock
            .Setup(x => x.StringGetAsync(
                "url:abc123",
                It.IsAny<CommandFlags>()))
            .ReturnsAsync("https://www.example.com");

        var service = CreateService(
            context,
            redisMock.Object);

        // Act
        var result = await service.RedirectUrlAsync("abc123");

        // Assert
        Assert.Equal(
            "https://www.example.com",
            result);
    }

    [Fact]
    public async Task RedirectUrlAsync_WhenRedisReadFails_FallsBackToPostgres()
    {
        // Arrange
        await ClearUrlsAsync();

        await using var context = CreateDbContext();

        context.Urls.Add(new Url
        {
            OriginalUrl = "https://www.example.com",
            ShortCode = "abc123",
            CreatedAt = DateTime.UtcNow,
            ClickCount = 0
        });

        await context.SaveChangesAsync();

        var redisMock = new Mock<IConnectionMultiplexer>();
        var databaseMock = new Mock<IDatabase>();

        redisMock
            .Setup(x => x.GetDatabase(
                It.IsAny<int>(),
                It.IsAny<object?>()))
            .Returns(databaseMock.Object);

        databaseMock
            .Setup(x => x.StringGetAsync(
                "url:abc123",
                It.IsAny<CommandFlags>()))
            .ThrowsAsync(
                new RedisConnectionException(
                    ConnectionFailureType.UnableToConnect,
                    CommandFlags.None,
                    "Redis unavailable"));

        var service = CreateService(
            context,
            redisMock.Object);

        // Act
        var result = await service.RedirectUrlAsync("abc123");

        // Assert
        Assert.Equal(
            "https://www.example.com",
            result);

        await using var verificationContext = CreateDbContext();

        var savedUrl = await verificationContext.Urls
            .SingleAsync(x => x.ShortCode == "abc123");

        Assert.Equal(1, savedUrl.ClickCount);
    }

    [Fact]
    public async Task RedirectUrlAsync_WhenRedisWriteFails_StillReturnsUrl()
    {
        // Arrange
        await ClearUrlsAsync();

        await using var context = CreateDbContext();

        context.Urls.Add(new Url
        {
            OriginalUrl = "https://www.example.com",
            ShortCode = "abc123",
            CreatedAt = DateTime.UtcNow,
            ClickCount = 0
        });

        await context.SaveChangesAsync();

        var redisMock = new Mock<IConnectionMultiplexer>();
        var databaseMock = new Mock<IDatabase>();

        redisMock
            .Setup(x => x.GetDatabase(
                It.IsAny<int>(),
                It.IsAny<object?>()))
            .Returns(databaseMock.Object);

        databaseMock
            .Setup(x => x.StringGetAsync(
                "url:abc123",
                It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisValue.Null);

        databaseMock
            .Setup(x => x.StringSetAsync(
                It.IsAny<RedisKey>(),
                It.IsAny<RedisValue>()))
            .ThrowsAsync(
                new RedisConnectionException(
                    ConnectionFailureType.UnableToConnect,
                    CommandFlags.None,
                    "Redis unavailable"));

        var service = CreateService(
            context,
            redisMock.Object);

        // Act
        var result = await service.RedirectUrlAsync("abc123");

        // Assert
        Assert.Equal(
            "https://www.example.com",
            result);

        await using var verificationContext = CreateDbContext();

        var savedUrl = await verificationContext.Urls
            .SingleAsync(x => x.ShortCode == "abc123");

        Assert.Equal(1, savedUrl.ClickCount);
    }

    [Fact]
    public async Task RedirectUrlAsync_WhenRedisReadFails_LogsWarning()
    {
        // Arrange
        await ClearUrlsAsync();

        await using var context = CreateDbContext();

        context.Urls.Add(new Url
        {
            OriginalUrl = "https://www.example.com",
            ShortCode = "abc123",
            CreatedAt = DateTime.UtcNow,
            ClickCount = 0
        });

        await context.SaveChangesAsync();

        var redisMock = new Mock<IConnectionMultiplexer>();
        var databaseMock = new Mock<IDatabase>();
        var loggerMock = new Mock<ILogger<UrlShortenerService>>();

        redisMock
            .Setup(x => x.GetDatabase(
                It.IsAny<int>(),
                It.IsAny<object?>()))
            .Returns(databaseMock.Object);

        databaseMock
            .Setup(x => x.StringGetAsync(
                "url:abc123",
                It.IsAny<CommandFlags>()))
            .ThrowsAsync(
                new RedisConnectionException(
                    ConnectionFailureType.UnableToConnect,
                    CommandFlags.None,
                    "Redis unavailable"));

        var service = CreateService(
            context,
            redisMock.Object,
            loggerMock.Object);

        // Act
        var result = await service.RedirectUrlAsync("abc123");

        // Assert
        Assert.Equal(
            "https://www.example.com",
            result);

        loggerMock.Verify(
            x => x.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>(
                    (state, type) =>
                        state.ToString()!.Contains(
                            "Redis read failed")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task RedirectUrlAsync_WhenRedisWriteFails_LogsWarning()
    {
        // Arrange
        await ClearUrlsAsync();

        await using var context = CreateDbContext();

        context.Urls.Add(new Url
        {
            OriginalUrl = "https://www.example.com",
            ShortCode = "abc123",
            CreatedAt = DateTime.UtcNow,
            ClickCount = 0
        });

        await context.SaveChangesAsync();

        var redisMock = new Mock<IConnectionMultiplexer>();
        var databaseMock = new Mock<IDatabase>();
        var loggerMock = new Mock<ILogger<UrlShortenerService>>();

        redisMock
            .Setup(x => x.GetDatabase(
                It.IsAny<int>(),
                It.IsAny<object?>()))
            .Returns(databaseMock.Object);

        databaseMock
            .Setup(x => x.StringGetAsync(
                "url:abc123",
                It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisValue.Null);

        databaseMock
            .Setup(x => x.StringSetAsync(
                It.IsAny<RedisKey>(),
                It.IsAny<RedisValue>()))
            .ThrowsAsync(
                new RedisConnectionException(
                    ConnectionFailureType.UnableToConnect,
                    CommandFlags.None,
                    "Redis unavailable"));

        var service = CreateService(
            context,
            redisMock.Object,
            loggerMock.Object);

        // Act
        var result = await service.RedirectUrlAsync("abc123");

        // Assert
        Assert.Equal(
            "https://www.example.com",
            result);

        await using var verificationContext = CreateDbContext();

        var savedUrl = await verificationContext.Urls
            .SingleAsync(x => x.ShortCode == "abc123");

        Assert.Equal(1, savedUrl.ClickCount);

        loggerMock.Verify(
            x => x.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>(
                    (state, type) =>
                        state.ToString()!.Contains(
                            "Redis write failed")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    private async Task<Url> serviceCreateUrlAsync(
        UrlShortenerDbContext context)
    {
        var service = CreateService(
            context,
            CreateRedisMock());

        return await service.CreateShortUrlAsync(
            "https://www.example.com");
    }
}
