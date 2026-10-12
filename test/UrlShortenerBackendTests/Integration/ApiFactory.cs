using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using UrlShortenerBackend.Tests.Integration;

namespace UrlShortenerBackend.Tests.Integration;

public class ApiFactory(
    PostgresFixture postgresFixture) : WebApplicationFactory<Program>
{
    private readonly string _kafkaTopic =
        $"url-clicked-test-{Guid.NewGuid():N}";

    private readonly string _kafkaConsumerGroup =
        $"click-count-test-consumer-{Guid.NewGuid():N}";

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
            _kafkaTopic);

        builder.UseSetting(
            "Kafka:ConsumerGroup",
            _kafkaConsumerGroup);
    }
}