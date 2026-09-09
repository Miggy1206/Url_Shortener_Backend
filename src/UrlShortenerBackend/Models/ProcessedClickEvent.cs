namespace UrlShortenerBackend.Api.Models;

public class ProcessedClickEvent
{
    public Guid EventId { get; set; }
    public DateTime ProcessedAt { get; set; }
}