# syntax=docker/dockerfile:1.7
#
# Two-stage, layer-aware build.
#
# Dependency resolution is its own RUN keyed only on the POMs, so editing application code
# re-runs javac without re-resolving the dependency graph. The BuildKit cache mount on
# /root/.m2 additionally shares the artifact cache across builds and across the three images
# in this repository, note it cannot be COPY'd into a later stage, because a cache mount is
# not part of the image filesystem.
#
# The runtime stage carries a JRE rather than a JDK and runs as a non-root user.

# --- Stage 1: build ------------------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build

# POMs first: this layer is invalidated by a dependency change and by nothing else.
COPY pom.xml .
COPY Infra/SchemaTool/pom.xml Infra/SchemaTool/
COPY Server/SkierServer/pom.xml Server/SkierServer/
COPY Consumer/SkierConsumer/pom.xml Consumer/SkierConsumer/
COPY Client/SkierClient/pom.xml Client/SkierClient/

# `|| true`: go-offline cannot always resolve every plugin dependency of a reactor build, and a
# partial warm cache is still worth having. The real build below is the authority.
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -pl Server/SkierServer -am dependency:go-offline -DskipTests || true

COPY Infra Infra
COPY Server Server

# Tests are not run here. A build that needs a Docker daemon inside a Docker build is a worse
# contract than one that produces exactly one artifact; CI runs the suites against real services.
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -pl Server/SkierServer -am package -DskipTests

# Exploding the fat jar lets Docker cache dependency layers separately from application classes,
# which are the only thing that changes between most deploys.
WORKDIR /staging
RUN cp /build/Server/SkierServer/target/skier-server.jar app.jar \
    && java -Djarmode=layertools -jar app.jar extract

# --- Stage 2: runtime ---------------------------------------------------------------------
# jammy rather than alpine: the alpine tag has no arm64 manifest, so an Apple-silicon build
# fails outright. Multi-arch support matters more here than the image size the musl base saves.
FROM eclipse-temurin:17-jre-jammy AS runtime

RUN groupadd --system skiers \
    && useradd --system --gid skiers --no-create-home skiers \
    && apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
USER skiers
WORKDIR /app

COPY --from=build --chown=skiers:skiers /staging/dependencies/ ./
COPY --from=build --chown=skiers:skiers /staging/spring-boot-loader/ ./
COPY --from=build --chown=skiers:skiers /staging/snapshot-dependencies/ ./
COPY --from=build --chown=skiers:skiers /staging/application/ ./

EXPOSE 8080

# MaxRAMPercentage rather than a fixed -Xmx: the container's memory limit is the only figure
# actually known at deploy time, and a hardcoded heap either wastes it or gets OOM-killed.
# ExitOnOutOfMemoryError so an exhausted heap becomes a restart rather than a zombie process
# that answers health checks but cannot serve.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError"

# Readiness, not liveness: this endpoint reports whether DynamoDB and the broker are reachable,
# so failing it should withhold traffic rather than restart the process.
HEALTHCHECK --interval=10s --timeout=3s --start-period=60s --retries=5 \
    CMD curl -fsS http://localhost:8080/actuator/health/readiness || exit 1

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS org.springframework.boot.loader.launch.JarLauncher"]
