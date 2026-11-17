# Same layout as the Quickstart consumer: build the jar with Maven, then run it on a JRE.
FROM public.ecr.aws/docker/library/maven:3.9-eclipse-temurin-17 AS build

WORKDIR /build
COPY pom.xml ./
COPY src ./src
RUN mvn --batch-mode --no-transfer-progress package

FROM public.ecr.aws/docker/library/eclipse-temurin:17-jre-jammy

WORKDIR /app
COPY --from=build --chown=10001:10001 /build/target/kcl-fulfillment.jar /app/kcl-fulfillment.jar

USER 10001:10001
ENTRYPOINT ["java", "-jar", "/app/kcl-fulfillment.jar"]
