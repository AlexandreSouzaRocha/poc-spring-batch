FROM registry.access.redhat.com/ubi9/openjdk-25:latest AS build
USER root
RUN microdnf install -y gzip tar && microdnf clean all
WORKDIR /app
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN ./mvnw -q -B -DskipTests dependency:resolve
COPY src/ src/
RUN ./mvnw -q -B -DskipTests package

FROM registry.access.redhat.com/ubi9/openjdk-25-runtime:latest
WORKDIR /app
COPY --from=build /app/target/saldo.batch-0.0.1-SNAPSHOT.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
