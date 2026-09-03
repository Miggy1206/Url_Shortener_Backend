namespace UrlShortenerBackend.Api.Models;

public class Url
{
    public int Id { get; set; }
    public required string OriginalUrl { get; set; }

    public required string ShortCode { get; set; }

    public DateTime CreatedAt { get; set; }

    public int ClickCount { get; set; }
    
}