FROM eclipse-temurin:17-jre

WORKDIR /app
ARG JAR_FILE=target/logistics-simulator-0.0.1-SNAPSHOT.jar
COPY ${JAR_FILE} app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar", "--spring.profiles.active=docker"]
