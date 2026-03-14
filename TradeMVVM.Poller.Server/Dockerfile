FROM mcr.microsoft.com/dotnet/runtime:8.0-bookworm-slim-arm64 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0-bullseye-slim-arm64 AS build
WORKDIR /src
COPY . ./
RUN dotnet restore "TradeMVVM.Poller.Server/TradeMVVM.Poller.Server.csproj"
RUN dotnet publish "TradeMVVM.Poller.Server/TradeMVVM.Poller.Server.csproj" -c Release -o /app/publish -r linux-arm64 --self-contained false

FROM base AS final
WORKDIR /app
COPY --from=build /app/publish ./
ENTRYPOINT ["dotnet", "TradeMVVM.Poller.Server.dll"]
