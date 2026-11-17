FROM public.ecr.aws/docker/library/maven:3.9-eclipse-temurin-17 AS build

WORKDIR /build
COPY pom.xml ./
COPY src ./src
RUN mvn --batch-mode --no-transfer-progress package

FROM public.ecr.aws/docker/library/eclipse-temurin:17-jre-jammy

RUN groupadd --gid 10001 consumer \
    && useradd --uid 10001 --gid 10001 --create-home --shell /usr/sbin/nologin consumer

WORKDIR /app
COPY --from=build --chown=10001:10001 /build/target/kinesis-consumer.jar /app/consumer.jar
COPY --chown=10001:10001 entrypoint.sh /app/entrypoint.sh
RUN chmod 0555 /app/entrypoint.sh

USER 10001:10001
ENTRYPOINT ["/app/entrypoint.sh"]
