namespace UrlShortenerBackend.Api.Models;

public class OutboxMessage
{
    public Guid Id { get; set; }

    public string Type { get; set; } = null!;

    public string Payload { get; set; } = null!;

    public DateTime OccurredAt { get; set; }

    public DateTime? PublishedAt { get; set; }

    public int AttemptCount { get; set; }

    public DateTime? LastAttemptAt { get; set; }

    public string? LastError { get; set; }

    public DateTime? ProcessingStartedAt { get; set; }
}