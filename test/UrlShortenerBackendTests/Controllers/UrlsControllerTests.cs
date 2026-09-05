using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using UrlShortenerBackend.Api.Controllers;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Models;
using UrlShortenerBackend.Api.Services;
using Moq;
using StackExchange.Redis;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace UrlShortenerBackend.Tests.Controllers;

public class UrlsControllerTests
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

   private static UrlsController CreateController(UrlShortenerDbContext context)
    {
        var service = new UrlShortenerService(
            context,
            CreateRedisMock(),
            NullLogger<UrlShortenerService>.Instance);

        var controller = new UrlsController(service);

        controller.ControllerContext = new ControllerContext
        {
            HttpContext = new DefaultHttpContext()
        };

        return controller;
    }

    [Fact]
    public async Task ShortenUrl_WithValidUrl_CreatesUrl()
    {
        // Arrange
        await using var context = CreateDbContext();
        var controller = CreateController(context);

        var request = new ShortenUrlRequest(
            "https://www.example.com");

        // Act
        var result = await controller.ShortenUrl(request);

        // Assert
        var createdResult = Assert.IsType<CreatedResult>(result);
        Assert.NotNull(createdResult.Value);

        var savedUrl = await context.Urls.SingleAsync();

        Assert.Equal("https://www.example.com", savedUrl.OriginalUrl);
        Assert.Equal(6, savedUrl.ShortCode.Length);
        Assert.Equal(0, savedUrl.ClickCount);
        Assert.NotEqual(default, savedUrl.CreatedAt);
    }

    [Fact]
    public async Task ShortenUrl_WithValidUrl_ReturnsCreated()
    {
        // Arrange
        await using var context = CreateDbContext();
        var controller = CreateController(context);

        var request = new ShortenUrlRequest(
            "https://www.example.com");

        // Act
        var result = await controller.ShortenUrl(request);

        // Assert
        Assert.IsType<CreatedResult>(result);
    }

    [Fact]
    public async Task ShortenUrl_WithValidUrl_ReturnsShortCode()
    {
        // Arrange
        await using var context = CreateDbContext();
        var controller = CreateController(context);

        var request = new ShortenUrlRequest(
            "https://www.example.com");

        // Act
        var result = await controller.ShortenUrl(request);

        // Assert
        var createdResult = Assert.IsType<CreatedResult>(result);
        Assert.NotNull(createdResult.Value);

        var value = createdResult.Value;

        var shortCode = value
            .GetType()
            .GetProperty("shortCode")?
            .GetValue(value)?
            .ToString();

        Assert.NotNull(shortCode);
        Assert.Equal(6, shortCode.Length);
    }

    [Fact]
    public async Task ShortenUrl_MultipleUrls_GeneratesUniqueShortCodes()
    {
        // Arrange
        await using var context = CreateDbContext();
        var controller = CreateController(context);

        // Act
        for (var i = 0; i < 100; i++)
        {
            var request = new ShortenUrlRequest(
                $"https://example.com/{i}");

            await controller.ShortenUrl(request);
        }

        // Assert
        var urls = await context.Urls.ToListAsync();

        Assert.Equal(100, urls.Count);
        Assert.Equal(
            100,
            urls.Select(x => x.ShortCode).Distinct().Count());
    }

    [Fact]
    public async Task RedirectToUrl_WithExistingShortCode_ReturnsRedirect()
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

        var controller = CreateController(context);

        // Act
        var result = await controller.RedirectToUrl("abc123");

        // Assert
        var redirectResult = Assert.IsType<RedirectResult>(result);

        Assert.Equal(
            "https://www.example.com",
            redirectResult.Url);
    }

    [Fact]
    public async Task RedirectToUrl_WithNonExistingShortCode_ReturnsNotFound()
    {
        // Arrange
        await using var context = CreateDbContext();

        var controller = CreateController(context);

        // Act
        var result = await controller.RedirectToUrl("nonexistent");

        // Assert
        Assert.IsType<NotFoundResult>(result);
    }

    [Fact]
    public async Task RedirectToUrl_WithExistingShortCode_IncrementsClickCount()
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

        var controller = CreateController(context);

        // Act
        await controller.RedirectToUrl("abc123");

        // Assert
        var url = await context.Urls
            .SingleAsync(x => x.ShortCode == "abc123");

        Assert.Equal(1, url.ClickCount);
    }
}