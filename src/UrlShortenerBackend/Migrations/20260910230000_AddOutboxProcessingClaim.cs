using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace UrlShortenerBackend.Migrations
{
    /// <inheritdoc />
    public partial class AddOutboxProcessingClaim : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<DateTime>(
                name: "ProcessingStartedAt",
                table: "OutboxMessages",
                type: "timestamp with time zone",
                nullable: true);

            migrationBuilder.CreateIndex(
                name: "IX_OutboxMessages_PublishedAt_ProcessingStartedAt_OccurredAt",
                table: "OutboxMessages",
                columns: new[] { "PublishedAt", "ProcessingStartedAt", "OccurredAt" });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_OutboxMessages_PublishedAt_ProcessingStartedAt_OccurredAt",
                table: "OutboxMessages");

            migrationBuilder.DropColumn(
                name: "ProcessingStartedAt",
                table: "OutboxMessages");
        }
    }
}
