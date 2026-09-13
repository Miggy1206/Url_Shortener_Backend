using Microsoft.EntityFrameworkCore;
using UrlShortenerBackend.Api.Models;

namespace UrlShortenerBackend.Api.Data;

public class UrlShortenerDbContext(DbContextOptions<UrlShortenerDbContext> options) : DbContext(options)
{
    public DbSet<Url> Urls => Set<Url>();

    public DbSet<ProcessedClickEvent> ProcessedClickEvents =>
        Set<ProcessedClickEvent>();

    public DbSet<OutboxMessage> OutboxMessages =>
        Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Url>()
            .HasIndex(x => x.ShortCode)
            .IsUnique();

        modelBuilder.Entity<ProcessedClickEvent>()
            .HasKey(x => x.EventId);

        modelBuilder.Entity<OutboxMessage>()
            .HasKey(x => x.Id);

        modelBuilder.Entity<OutboxMessage>()
            .HasIndex(x => x.PublishedAt);

        modelBuilder.Entity<OutboxMessage>()
            .HasIndex(x => new
            {
                x.PublishedAt,
                x.OccurredAt
            });

        modelBuilder.Entity<OutboxMessage>()
            .HasIndex(x => new
            {
                x.PublishedAt,
                x.ProcessingStartedAt,
                x.OccurredAt
            });
    }
}