using Microsoft.EntityFrameworkCore;

using UrlShortenerBackend.Api.Models;

namespace UrlShortenerBackend.Api.Data;

public class UrlShortenerDbContext(DbContextOptions<UrlShortenerDbContext> options) : DbContext(options)
{
    public DbSet<Url> Urls => Set<Url>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    modelBuilder.Entity<Url>()
        .HasIndex(x => x.ShortCode)
        .IsUnique();
}
}