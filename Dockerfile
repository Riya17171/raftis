# Build: compile with the Gradle wrapper inside the image, so nothing but Docker is needed.
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle gradle
RUN ./gradlew --no-daemon --quiet --version > /dev/null
COPY src/main src/main
RUN ./gradlew --no-daemon --quiet installDist

# Run: just a JRE and the app.
FROM eclipse-temurin:21-jre
COPY --from=build /src/build/install/raftis /opt/raftis
VOLUME /data
EXPOSE 6380 7380
ENTRYPOINT ["/opt/raftis/bin/raftis", "--bind", "0.0.0.0", "--dir", "/data"]
