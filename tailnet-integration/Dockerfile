FROM gradle:jdk25 AS build
WORKDIR /src
COPY . .
RUN ./gradlew --no-daemon :tailnet-integration:installDist

FROM eclipse-temurin:21-jre
COPY --from=build /src/tailnet-integration/build/install/tailnet-integration /opt/vgi-tailnet-java
ENTRYPOINT ["/opt/vgi-tailnet-java/bin/tailnet-integration"]
