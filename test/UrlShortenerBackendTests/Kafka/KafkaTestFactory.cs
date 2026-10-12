using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using UrlShortenerBackend.Tests.Integration;

namespace UrlShortenerBackend.Tests.Kafka;

public class KafkaTestFactory(
    PostgresFixture postgresFixture)
    : WebApplicationFactory<Program>
{
    public string TopicName { get; } =
        $"url-clicked-test-{Guid.NewGuid():N}";

    protected override void ConfigureWebHost(
        IWebHostBuilder builder)
    {
        builder.UseSetting(
            "ConnectionStrings:DefaultConnection",
            postgresFixture.ConnectionString);

        builder.UseSetting(
            "Redis:ConnectionString",
            "localhost:6379");

        builder.UseSetting(
            "Kafka:BootstrapServers",
            "localhost:9093");

        builder.UseSetting(
            "Kafka:ClickTopic",
            TopicName);

        builder.UseSetting(
            "Kafka:ConsumerGroup",
            $"click-count-consumer-test-{Guid.NewGuid():N}");
    }
}