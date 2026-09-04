using UrlShortenerBackend.Api.Models;

namespace UrlShortenerBackend.Api.Services;

public interface IUrlShortenerService
{
    Task<Url> CreateShortUrlAsync(string originalUrl);
    Task<string?> RedirectUrlAsync(string shortCode);

}