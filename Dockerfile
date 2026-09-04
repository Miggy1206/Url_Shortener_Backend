FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

WORKDIR /src

COPY src/UrlShortenerBackend/UrlShortenerBackend.csproj src/UrlShortenerBackend/

RUN dotnet restore src/UrlShortenerBackend/UrlShortenerBackend.csproj

COPY src/UrlShortenerBackend/ src/UrlShortenerBackend/

RUN dotnet publish src/UrlShortenerBackend/UrlShortenerBackend.csproj \
    -c Release \
    -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled AS runtime

WORKDIR /app

COPY --from=build /app/publish .

EXPOSE 8080

ENTRYPOINT ["dotnet", "UrlShortenerBackend.dll"]

