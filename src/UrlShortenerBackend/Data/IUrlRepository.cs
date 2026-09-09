using UrlShortenerBackend.Api.Models;

namespace UrlShortenerBackend.Api.Data;

public interface IUrlRepository
{
    Task IncrementClickCountAsync(
        string shortCode,
        CancellationToken cancellationToken = default);
}