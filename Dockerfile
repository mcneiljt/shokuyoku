FROM docker.io/library/maven:3-openjdk-17
COPY src/ ./src/
COPY pom.xml ./pom.xml
RUN mvn package

FROM gcr.io/distroless/java17-debian11
COPY --from=0 target/shokuyoku-1.0.0.jar ./
ENTRYPOINT ["java", "-jar", "shokuyoku-1.0.0.jar"]
