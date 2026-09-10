using Microsoft.EntityFrameworkCore;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Services;
using StackExchange.Redis;
using System.Threading.RateLimiting;
using Confluent.Kafka;
using UrlShortenerBackend.Api.Kafka;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using UrlShortenerBackend.Api.Observability;
using OpenTelemetry.Trace;
using Polly;
using Polly.CircuitBreaker;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services
    .AddOpenTelemetry()
    .ConfigureResource(resource =>
        resource.AddService(
            serviceName: "UrlShortenerBackend",
            serviceVersion: "1.0.0"))
    .WithMetrics(metrics =>
    {
        metrics
            .AddAspNetCoreInstrumentation()
            .AddRuntimeInstrumentation()
            .AddProcessInstrumentation()
            .AddMeter(UrlShortenerMetrics.MeterName)
            .AddPrometheusExporter();
    })
    .WithTracing(tracing =>
    {
        tracing
            .AddAspNetCoreInstrumentation()
            .AddRedisInstrumentation()
            .AddSource(UrlShortenerActivitySource.Name)
            .AddOtlpExporter(options =>
            {
                var endpoint =
                    builder.Configuration["OpenTelemetry:Endpoint"]
                    ?? "http://localhost:4318";

                options.Endpoint = new Uri(endpoint);
            });
    });

builder.Services.AddOpenApi();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddControllers();
builder.Services.AddHealthChecks();
builder.Services.AddScoped<IUrlShortenerService, UrlShortenerService>();
builder.Services.AddProblemDetails();
builder.Services.AddScoped<IUrlRepository, UrlRepository>();
builder.Services.AddScoped<IClickEventProcessor, ClickEventProcessor>();
builder.Services.AddHostedService<ClickEventConsumer>();

builder.Services.AddRateLimiter(options =>
{
    options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;

    options.AddPolicy("url-creation", httpContext =>
        RateLimitPartition.GetFixedWindowLimiter(
            partitionKey: httpContext.Connection.RemoteIpAddress?.ToString() ?? "unknown",
            factory: _ => new FixedWindowRateLimiterOptions
            {
                PermitLimit = 5,
                Window = TimeSpan.FromMinutes(1),
                QueueLimit = 0,
                AutoReplenishment = true
            }));

    options.AddPolicy("url-redirect", httpContext =>
        RateLimitPartition.GetFixedWindowLimiter(
            partitionKey: httpContext.Connection.RemoteIpAddress?.ToString() ?? "unknown",
            factory: _ => new FixedWindowRateLimiterOptions
            {
                PermitLimit = 60,
                Window = TimeSpan.FromMinutes(1),
                QueueLimit = 0,
                AutoReplenishment = true
            }));
});

builder.Services.AddDbContext<UrlShortenerDbContext>(options =>
    options.UseNpgsql(
        builder.Configuration.GetConnectionString("DefaultConnection")));

builder.Services.AddSingleton<IConnectionMultiplexer>(
    ConnectionMultiplexer.Connect(
        builder.Configuration["Redis:ConnectionString"]!));

builder.Services.AddSingleton<IProducer<string, string>>(
    serviceProvider =>
    {
        var configuration =
            serviceProvider.GetRequiredService<IConfiguration>();

        var bootstrapServers =
            configuration["Kafka:BootstrapServers"]
            ?? throw new InvalidOperationException(
                "Kafka:BootstrapServers is not configured.");

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            MessageTimeoutMs = 1000,
            RequestTimeoutMs = 500
        };

        return new ProducerBuilder<string, string>(
            producerConfig)
            .Build();
    });



builder.Services.AddScoped<IClickEventProducer, ClickEventProducer>();

builder.Services.AddResiliencePipeline(
    "kafka-publish",
    pipeline =>
    {
        pipeline.AddCircuitBreaker(
            new CircuitBreakerStrategyOptions
            {
                FailureRatio = 0.5,
                SamplingDuration = TimeSpan.FromSeconds(30),
                MinimumThroughput = 2,
                BreakDuration = TimeSpan.FromSeconds(30),

                OnOpened = args =>
                {
                    UrlShortenerMetrics.KafkaCircuitOpened.Add(1);

                    return default;
                },

                OnClosed = args =>
                {
                    UrlShortenerMetrics.KafkaCircuitClosed.Add(1);

                    return default;
                },

                OnHalfOpened = args =>
                {
                    UrlShortenerMetrics.KafkaCircuitHalfOpened.Add(1);

                    return default;
                }
            });
    });

var app = builder.Build();

app.MapHealthChecks("/healthz");

app.MapPrometheusScrapingEndpoint();

app.UseExceptionHandler();

app.UseRouting();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
    app.MapOpenApi();
}

if (!app.Environment.IsDevelopment())
{
    app.UseHttpsRedirection();
}

app.UseRateLimiter();

app.MapControllers();

app.Run();