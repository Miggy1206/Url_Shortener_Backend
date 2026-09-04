using Microsoft.EntityFrameworkCore;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Services;
using Moq;
using StackExchange.Redis;

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

    [Fact]
    public async Task CreateShortUrlAsync_WithValidUrl_CreatesUrl()
    {
        await using var context = CreateDbContext();
        var service = new UrlShortenerService(context, CreateRedisMock());

        var result = await service.CreateShortUrlAsync(
            "https://www.example.com");

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
        await using var context = CreateDbContext();
        var service = new UrlShortenerService(context, CreateRedisMock());

        var first = await service.CreateShortUrlAsync(
            "https://www.example.com/1");

        var second = await service.CreateShortUrlAsync(
            "https://www.example.com/2");

        Assert.NotEqual(first.ShortCode, second.ShortCode);
    }

    [Fact]
    public async Task RedirectUrlAsync_WithExistingShortCode_ReturnsUrl()
    {
        await using var context = CreateDbContext();
        var service = new UrlShortenerService(context, CreateRedisMock());

        var created = await service.CreateShortUrlAsync(
            "https://www.example.com");

        var result = await service.RedirectUrlAsync(
            created.ShortCode);

        Assert.NotNull(result);
        Assert.Equal(
            created.OriginalUrl,
            result);
    }

    [Fact]
    public async Task RedirectUrlAsync_WithExistingShortCode_IncrementsClickCount()
    {
        await using var context = CreateDbContext();
        var service = new UrlShortenerService(context, CreateRedisMock());

        var created = await service.CreateShortUrlAsync(
            "https://www.example.com");

        await service.RedirectUrlAsync(created.ShortCode);

        var savedUrl = await context.Urls
            .SingleAsync(x => x.ShortCode == created.ShortCode);

        Assert.Equal(1, savedUrl.ClickCount);
    }

    [Fact]
    public async Task RedirectUrlAsync_WithUnknownShortCode_ReturnsNull()
    {
        await using var context = CreateDbContext();
        var service = new UrlShortenerService(context, CreateRedisMock());

        var result = await service.RedirectUrlAsync("missing");

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

        var service = new UrlShortenerService(
            context,
            redisMock.Object);

        // Act
        var result = await service.RedirectUrlAsync("abc123");

        // Assert
        Assert.Equal(
            "https://www.example.com",
            result);
    }
}