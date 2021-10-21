FROM maven:3-openjdk-8
COPY src/ ./src/
COPY pom.xml ./pom.xml
RUN mvn package

FROM gcr.io/distroless/java:8-debug
COPY --from=0 target/shokuyoku-1.0.0.jar ./
ENTRYPOINT ["java", "-jar", "shokuyoku-1.0.0.jar"]
