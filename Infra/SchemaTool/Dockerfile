# syntax=docker/dockerfile:1.7
#
# One-shot schema applier. Compose runs this to completion before the services start,
# which is what makes `make up` produce a working system rather than two containers
# crash-looping against tables that do not exist yet.

FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build

COPY pom.xml .
COPY Infra/SchemaTool/pom.xml Infra/SchemaTool/
COPY Server/SkierServer/pom.xml Server/SkierServer/
COPY Consumer/SkierConsumer/pom.xml Consumer/SkierConsumer/
COPY Client/SkierClient/pom.xml Client/SkierClient/
COPY Infra Infra

RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -pl Infra/SchemaTool -am package -DskipTests

# jammy rather than alpine: the alpine tag has no arm64 manifest, so an Apple-silicon
# build fails outright. Multi-arch matters more here than the ~60MB the musl image saves.
FROM eclipse-temurin:17-jre-jammy AS runtime
RUN groupadd --system skiers \
    && useradd --system --gid skiers --no-create-home skiers
USER skiers
WORKDIR /app
COPY --from=build --chown=skiers:skiers /build/Infra/SchemaTool/target/skier-schema-tool.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
