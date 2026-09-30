FROM mcr.microsoft.com/dotnet/sdk:10.0.400 AS build
WORKDIR /src
ADD https://github.com/microsoft/garnet/archive/refs/tags/v2.1.8.tar.gz /tmp/garnet.tar.gz
RUN tar -xzf /tmp/garnet.tar.gz --strip-components=1 \
    && dotnet build modules/GarnetJSON/GarnetJSON.csproj -c Release -f net10.0 -o /out

FROM ghcr.io/microsoft/garnet:2.1.8
COPY --from=build /out/GarnetJSON.dll /app/modules/GarnetJSON.dll
