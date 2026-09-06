using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Moq;
using UrlShortenerBackend.Api.Controllers;
using UrlShortenerBackend.Api.Models;
using UrlShortenerBackend.Api.Services;

namespace UrlShortenerBackend.Tests.Controllers;

public class UrlsControllerTests
{
    private static UrlsController CreateController(
        IUrlShortenerService service)
    {
        var controller = new UrlsController(service);

        controller.ControllerContext = new ControllerContext
        {
            HttpContext = new DefaultHttpContext()
        };

        return controller;
    }

    [Fact]
    public async Task ShortenUrl_WithValidUrl_ReturnsCreated()
    {
        // Arrange
        var serviceMock = new Mock<IUrlShortenerService>();

        serviceMock
            .Setup(x => x.CreateShortUrlAsync(
                "https://www.example.com"))
            .ReturnsAsync(new Url
            {
                OriginalUrl = "https://www.example.com",
                ShortCode = "abc123",
                CreatedAt = DateTime.UtcNow,
                ClickCount = 0
            });

        var controller = CreateController(
            serviceMock.Object);

        var request = new ShortenUrlRequest(
            "https://www.example.com");

        // Act
        var result = await controller.ShortenUrl(request);

        // Assert
        var createdResult = Assert.IsType<CreatedResult>(result);

        Assert.NotNull(createdResult.Value);
        Assert.Equal(
            "/abc123",
            createdResult.Location);
    }

    [Fact]
    public async Task ShortenUrl_WithValidUrl_ReturnsShortCode()
    {
        // Arrange
        var serviceMock = new Mock<IUrlShortenerService>();

        serviceMock
            .Setup(x => x.CreateShortUrlAsync(
                "https://www.example.com"))
            .ReturnsAsync(new Url
            {
                OriginalUrl = "https://www.example.com",
                ShortCode = "abc123",
                CreatedAt = DateTime.UtcNow,
                ClickCount = 0
            });

        var controller = CreateController(
            serviceMock.Object);

        var request = new ShortenUrlRequest(
            "https://www.example.com");

        // Act
        var result = await controller.ShortenUrl(request);

        // Assert
        var createdResult = Assert.IsType<CreatedResult>(result);

        Assert.NotNull(createdResult.Value);

        var shortCode = createdResult.Value
            .GetType()
            .GetProperty("shortCode")?
            .GetValue(createdResult.Value)?
            .ToString();

        Assert.Equal(
            "abc123",
            shortCode);
    }

    [Fact]
    public async Task ShortenUrl_WithValidUrl_CallsService()
    {
        // Arrange
        var serviceMock = new Mock<IUrlShortenerService>();

        serviceMock
            .Setup(x => x.CreateShortUrlAsync(
                "https://www.example.com"))
            .ReturnsAsync(new Url
            {
                OriginalUrl = "https://www.example.com",
                ShortCode = "abc123",
                CreatedAt = DateTime.UtcNow,
                ClickCount = 0
            });

        var controller = CreateController(
            serviceMock.Object);

        var request = new ShortenUrlRequest(
            "https://www.example.com");

        // Act
        await controller.ShortenUrl(request);

        // Assert
        serviceMock.Verify(
            x => x.CreateShortUrlAsync(
                "https://www.example.com"),
            Times.Once);
    }

    [Fact]
    public async Task RedirectToUrl_WithExistingShortCode_ReturnsRedirect()
    {
        // Arrange
        var serviceMock = new Mock<IUrlShortenerService>();

        serviceMock
            .Setup(x => x.RedirectUrlAsync("abc123"))
            .ReturnsAsync("https://www.example.com");

        var controller = CreateController(
            serviceMock.Object);

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
        var serviceMock = new Mock<IUrlShortenerService>();

        serviceMock
            .Setup(x => x.RedirectUrlAsync("nonexistent"))
            .ReturnsAsync((string?)null);

        var controller = CreateController(
            serviceMock.Object);

        // Act
        var result = await controller.RedirectToUrl(
            "nonexistent");

        // Assert
        Assert.IsType<NotFoundResult>(result);
    }

    [Fact]
    public async Task RedirectToUrl_WithExistingShortCode_CallsService()
    {
        // Arrange
        var serviceMock = new Mock<IUrlShortenerService>();

        serviceMock
            .Setup(x => x.RedirectUrlAsync("abc123"))
            .ReturnsAsync("https://www.example.com");

        var controller = CreateController(
            serviceMock.Object);

        // Act
        await controller.RedirectToUrl("abc123");

        // Assert
        serviceMock.Verify(
            x => x.RedirectUrlAsync("abc123"),
            Times.Once);
    }
}
