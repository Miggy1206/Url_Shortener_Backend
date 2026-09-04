using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using UrlShortenerBackend.Api.Controllers;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Models;

namespace UrlShortenerBackend.Tests.Controllers;

public class UrlsControllerTests
{
    private static UrlShortenerDbContext CreateDbContext()
    {
        var options = new DbContextOptionsBuilder<UrlShortenerDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString())
            .Options;

        return new UrlShortenerDbContext(options);
    }

    private static UrlsController CreateController(UrlShortenerDbContext context)
    {
        var controller = new UrlsController(context);

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
        var okResult = Assert.IsType<OkObjectResult>(result);
        Assert.NotNull(okResult.Value);

        var savedUrl = await context.Urls.SingleAsync();

        Assert.Equal("https://www.example.com", savedUrl.OriginalUrl);
        Assert.Equal(6, savedUrl.ShortCode.Length);
        Assert.Equal(0, savedUrl.ClickCount);
        Assert.NotEqual(default, savedUrl.CreatedAt);
    }

    [Fact]
    public async Task ShortenUrl_WithValidUrl_ReturnsOk()
    {
        // Arrange
        await using var context = CreateDbContext();
        var controller = CreateController(context);

        var request = new ShortenUrlRequest(
            "https://www.example.com");

        // Act
        var result = await controller.ShortenUrl(request);

        // Assert
        Assert.IsType<OkObjectResult>(result);
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
        var okResult = Assert.IsType<OkObjectResult>(result);
        Assert.NotNull(okResult.Value);
        var value = okResult.Value;

        var shortCode = value
            .GetType()
            .GetProperty("shortCode")?
            .GetValue(value)?
            .ToString();

        Assert.NotNull(shortCode);
        Assert.Equal(6, shortCode.Length);
    }

    [Fact]
    public async Task RedirectToUrl_WithExistingShortCode_ReturnsRedirect()
    {
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

        var result = await controller.RedirectToUrl("abc123");

        var redirectResult = Assert.IsType<RedirectResult>(result);

        Assert.Equal("https://www.example.com", redirectResult.Url);
    }

    [Fact]
    public async Task RedirectToUrl_WithNonExistingShortCode_ReturnsNotFound()
    {
        await using var context = CreateDbContext();

        var controller = CreateController(context);

        var result = await controller.RedirectToUrl("nonexistent");

        Assert.IsType<NotFoundResult>(result);
    }

    [Fact]
    public async Task RedirectToUrl_WithExistingShortCode_IncrementsClickCount()
    {
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

        await controller.RedirectToUrl("abc123");

        var url = await context.Urls.SingleAsync(x => x.ShortCode == "abc123");

        Assert.Equal(1, url.ClickCount);


    }
}