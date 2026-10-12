using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.AspNetCore.Mvc.Testing;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Models;

namespace UrlShortenerBackend.Tests.Integration;

[Collection("Integration tests")]
public class UrlsApiTests :
    IClassFixture<PostgresFixture>,
    IDisposable
{
    private readonly PostgresFixture _postgres;
    private readonly ApiFactory _factory;
    private readonly HttpClient _client;

    public UrlsApiTests(
        PostgresFixture postgresFixture)
    {
        _postgres = postgresFixture;

        _factory =
            new ApiFactory(
                postgresFixture);

        _client =
            _factory.CreateClient(
                new WebApplicationFactoryClientOptions
                {
                    AllowAutoRedirect = false
                });
    }

    [Fact]
    public async Task CreateShortUrl_WithValidUrl_ReturnsOk()
    {
        var request = new
        {
            originalUrl = "https://www.example.com"
        };

        var response =
            await _client.PostAsJsonAsync(
                "/api/urls",
                request);

        Assert.Equal(
            HttpStatusCode.Created,
            response.StatusCode);
    }

    [Fact]
    public async Task CreateAndRedirectUrl_WorksEndToEnd()
    {
        var request = new
        {
            originalUrl = "https://www.example.com"
        };

        var createResponse =
            await _client.PostAsJsonAsync(
                "/api/urls",
                request);

        Assert.Equal(
            HttpStatusCode.Created,
            createResponse.StatusCode);

        var createResult =
            await createResponse.Content
                .ReadFromJsonAsync<JsonElement>();

        var shortCode =
            createResult
                .GetProperty("shortCode")
                .GetString();

        Assert.NotNull(shortCode);

        var redirectResponse =
            await _client.GetAsync(
                $"/{shortCode}");

        Assert.Equal(
            HttpStatusCode.Redirect,
            redirectResponse.StatusCode);

        Assert.Equal(
            "https://www.example.com/",
            redirectResponse.Headers.Location?.ToString());

        await WaitForClickCountAsync(
            shortCode,
            1);
    }

    [Fact]
    public async Task ShortenUrl_WithMissingUrl_ReturnsBadRequest()
    {
        var request = new
        {
            OriginalUrl = ""
        };

        var response =
            await _client.PostAsJsonAsync(
                "/api/urls",
                request);

        Assert.Equal(
            HttpStatusCode.BadRequest,
            response.StatusCode);
    }

    [Fact]
    public async Task ShortenUrl_WithInvalidUrl_ReturnsBadRequest()
    {
        var request = new
        {
            OriginalUrl = "not-a-url"
        };

        var response =
            await _client.PostAsJsonAsync(
                "/api/urls",
                request);

        Assert.Equal(
            HttpStatusCode.BadRequest,
            response.StatusCode);
    }

    [Fact]
    public async Task ShortenUrl_WithUnsupportedScheme_ReturnsBadRequest()
    {
        var request = new
        {
            originalUrl = "ftp://example.com/file.txt"
        };

        var response =
            await _client.PostAsJsonAsync(
                "/api/urls",
                request);

        Assert.Equal(
            HttpStatusCode.BadRequest,
            response.StatusCode);
    }

    [Fact]
    public async Task ShortenUrl_WithJavascriptUrl_ReturnsBadRequest()
    {
        var request = new
        {
            originalUrl = "javascript:alert(1)"
        };

        var response =
            await _client.PostAsJsonAsync(
                "/api/urls",
                request);

        Assert.Equal(
            HttpStatusCode.BadRequest,
            response.StatusCode);
    }

    [Fact]
    public async Task ShortenUrl_WithUrlExceedingMaxLength_ReturnsBadRequest()
    {
        var longUrl =
            $"https://example.com/{new string('a', 2048)}";

        var request = new
        {
            originalUrl = longUrl
        };

        var response =
            await _client.PostAsJsonAsync(
                "/api/urls",
                request);

        Assert.Equal(
            HttpStatusCode.BadRequest,
            response.StatusCode);
    }

    [Fact]
    public async Task CreateShortUrl_WhenRateLimitExceeded_ReturnsTooManyRequests()
    {
        for (var i = 0; i < 5; i++)
        {
            var request = new
            {
                originalUrl =
                    $"https://example.com/rate-limit/{Guid.NewGuid()}"
            };

            var response =
                await _client.PostAsJsonAsync(
                    "/api/urls",
                    request);

            Assert.Equal(
                HttpStatusCode.Created,
                response.StatusCode);
        }

        var limitedRequest = new
        {
            originalUrl =
                $"https://example.com/rate-limit/{Guid.NewGuid()}"
        };

        var limitedResponse =
            await _client.PostAsJsonAsync(
                "/api/urls",
                limitedRequest);

        Assert.Equal(
            HttpStatusCode.TooManyRequests,
            limitedResponse.StatusCode);
    }

    [Fact]
    public async Task RedirectToUrl_WhenRateLimitExceeded_ReturnsTooManyRequests()
    {
        var request = new
        {
            originalUrl =
                $"https://example.com/rate-limit/{Guid.NewGuid()}"
        };

        var createResponse =
            await _client.PostAsJsonAsync(
                "/api/urls",
                request);

        Assert.Equal(
            HttpStatusCode.Created,
            createResponse.StatusCode);

        var createResult =
            await createResponse.Content
                .ReadFromJsonAsync<JsonElement>();

        var shortCode =
            createResult
                .GetProperty("shortCode")
                .GetString();

        Assert.NotNull(shortCode);

        for (var i = 0; i < 60; i++)
        {
            var response =
                await _client.GetAsync(
                    $"/{shortCode}");

            Assert.Equal(
                HttpStatusCode.Redirect,
                response.StatusCode);
        }

        var limitedResponse =
            await _client.GetAsync(
                $"/{shortCode}");

        Assert.Equal(
            HttpStatusCode.TooManyRequests,
            limitedResponse.StatusCode);
    }

    [Fact]
    public async Task RedirectUrl_WithConcurrentRequests_PersistsAllClicks()
    {
        var request = new
        {
            originalUrl = "https://www.example.com"
        };

        var createResponse =
            await _client.PostAsJsonAsync(
                "/api/urls",
                request);

        Assert.Equal(
            HttpStatusCode.Created,
            createResponse.StatusCode);

        var createResult =
            await createResponse.Content
                .ReadFromJsonAsync<JsonElement>();

        var shortCode =
            createResult
                .GetProperty("shortCode")
                .GetString();

        Assert.NotNull(shortCode);

        const int requestCount = 50;

        var tasks =
            Enumerable.Range(0, requestCount)
                .Select(
                    _ => _client.GetAsync(
                        $"/{shortCode}"));

        var responses =
            await Task.WhenAll(tasks);

        Assert.All(
            responses,
            response => Assert.Equal(
                HttpStatusCode.Redirect,
                response.StatusCode));

        await WaitForClickCountAsync(
            shortCode,
            requestCount);
    }

    private async Task WaitForClickCountAsync(
        string shortCode,
        int expectedClickCount,
        TimeSpan? timeout = null)
    {
        var deadline =
            DateTime.UtcNow +
            (timeout ?? TimeSpan.FromSeconds(10));

        while (DateTime.UtcNow < deadline)
        {
            await using var context =
                CreateDbContext();

            var clickCount =
                await context.Urls
                    .AsNoTracking()
                    .Where(x => x.ShortCode == shortCode)
                    .Select(x => x.ClickCount)
                    .SingleAsync();

            if (clickCount == expectedClickCount)
            {
                return;
            }

            await Task.Delay(100);
        }

        await using var finalContext =
            CreateDbContext();

        var finalClickCount =
            await finalContext.Urls
                .AsNoTracking()
                .Where(x => x.ShortCode == shortCode)
                .Select(x => x.ClickCount)
                .SingleAsync();

        Assert.Equal(
            expectedClickCount,
            finalClickCount);
    }

    private UrlShortenerDbContext CreateDbContext()
    {
        var options =
            new DbContextOptionsBuilder<UrlShortenerDbContext>()
                .UseNpgsql(
                    _postgres.ConnectionString)
                .Options;

        return new UrlShortenerDbContext(
            options);
    }

    public void Dispose()
    {
        _client.Dispose();
        _factory.Dispose();
    }
}
