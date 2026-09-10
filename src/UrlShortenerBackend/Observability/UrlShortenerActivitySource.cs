using System.Diagnostics;

namespace UrlShortenerBackend.Api.Observability;

public static class UrlShortenerActivitySource
{
    public const string Name = "UrlShortenerBackend";

    public static readonly ActivitySource Source =
        new(Name);
}