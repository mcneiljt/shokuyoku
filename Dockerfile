FROM docker.io/node:17-buster as ui
COPY ui/ ./ui/
RUN cd ui && npm install && npm run build

FROM docker.io/library/maven:3-openjdk-17 as maven
COPY src/ ./src/
COPY pom.xml ./pom.xml
RUN mvn package -Dmaven.test.skip

FROM gcr.io/distroless/java17-debian11:debug
USER nonroot
WORKDIR /home/nonroot
COPY --from=maven target/shokuyoku-1.0.0.jar ./
COPY --from=ui --chown=nonroot ui/build /ui/
ENTRYPOINT ["java", "-jar", "shokuyoku-1.0.0.jar"]
