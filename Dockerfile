FROM docker.io/node:17-buster
COPY ui/ ./ui/
RUN cd ui && npm install && npm run build

FROM docker.io/library/maven:3-openjdk-17
COPY src/ ./src/
COPY pom.xml ./pom.xml
RUN mvn package -Dmaven.test.skip

FROM gcr.io/distroless/java17-debian11:debug
COPY --from=1 target/shokuyoku-1.0.0.jar ./
COPY --from=0 ui/build /ui/
ENTRYPOINT ["java", "-jar", "shokuyoku-1.0.0.jar"]
