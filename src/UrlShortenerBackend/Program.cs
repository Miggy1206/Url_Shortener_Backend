using Microsoft.EntityFrameworkCore;
using UrlShortenerBackend.Api.Data;

var builder = WebApplication.CreateBuilder(args);

Console.WriteLine(
    $"Connection string: {builder.Configuration.GetConnectionString("DefaultConnection")}");

// Add services to the container.
builder.Services.AddOpenApi();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddControllers();
builder.Services.AddHealthChecks();

builder.Services.AddDbContext<UrlShortenerDbContext>(options =>
    options.UseNpgsql(
        builder.Configuration.GetConnectionString("DefaultConnection")));

var app = builder.Build();

app.MapHealthChecks("/healthz");

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

app.MapControllers();

app.Run();