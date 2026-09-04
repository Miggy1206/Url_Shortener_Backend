using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace UrlShortenerBackend.Migrations
{
    /// <inheritdoc />
    public partial class AddUniqueShortCodeIndex : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateIndex(
                name: "IX_Urls_ShortCode",
                table: "Urls",
                column: "ShortCode",
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_Urls_ShortCode",
                table: "Urls");
        }
    }
}
