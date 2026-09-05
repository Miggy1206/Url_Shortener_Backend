using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using StackExchange.Redis;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Models;
using UrlShortenerBackend.Api.Services;

namespace UrlShortenerBackend.Tests.Services;

public class UrlShortenerServiceTests
{
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

    private static UrlShortenerDbContext CreateDbContext()
    {
        var options = new DbContextOptionsBuilder<UrlShortenerDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString())
            .Options;

        return new UrlShortenerDbContext(options);
    }

    private static UrlShortenerService CreateService(
        UrlShortenerDbContext context,
        IConnectionMultiplexer redis)
    {
        return new UrlShortenerService(
            context,
            redis,
            NullLogger<UrlShortenerService>.Instance);
    }

    [Fact]
    public async Task CreateShortUrlAsync_WithValidUrl_CreatesUrl()
    {
        // Arrange
        await using var context = CreateDbContext();
        var service = CreateService(context, CreateRedisMock());

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
        await using var context = CreateDbContext();
        var service = CreateService(context, CreateRedisMock());

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
        await using var context = CreateDbContext();
        var service = CreateService(context, CreateRedisMock());

        var created = await service.CreateShortUrlAsync(
            "https://www.example.com");

        // Act
        var result = await service.RedirectUrlAsync(
            created.ShortCode);

        // Assert
        Assert.NotNull(result);

        Assert.Equal(
            created.OriginalUrl,
            result);
    }

    [Fact]
    public async Task RedirectUrlAsync_WithExistingShortCode_IncrementsClickCount()
    {
        // Arrange
        await using var context = CreateDbContext();
        var service = CreateService(context, CreateRedisMock());

        var created = await service.CreateShortUrlAsync(
            "https://www.example.com");

        // Act
        await service.RedirectUrlAsync(
            created.ShortCode);

        // Assert
        var savedUrl = await context.Urls
            .SingleAsync(x => x.ShortCode == created.ShortCode);

        Assert.Equal(1, savedUrl.ClickCount);
    }

    [Fact]
    public async Task RedirectUrlAsync_WithUnknownShortCode_ReturnsNull()
    {
        // Arrange
        await using var context = CreateDbContext();
        var service = CreateService(context, CreateRedisMock());

        // Act
        var result = await service.RedirectUrlAsync("missing");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task RedirectUrl_WithCachedUrl_ReturnsCachedUrl()
    {
        // Arrange
        await using var context = CreateDbContext();

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

        var savedUrl = await context.Urls
            .SingleAsync(x => x.ShortCode == "abc123");

        Assert.Equal(1, savedUrl.ClickCount);
    }

    [Fact]
    public async Task RedirectUrlAsync_WhenRedisWriteFails_StillReturnsUrl()
    {
        // Arrange
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
                "url:abc123",
                "https://www.example.com",
                It.IsAny<TimeSpan?>(),
                It.IsAny<bool>(),
                It.IsAny<When>(),
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

        var savedUrl = await context.Urls
            .SingleAsync(x => x.ShortCode == "abc123");

        Assert.Equal(1, savedUrl.ClickCount);
    }
}
