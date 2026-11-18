FROM maven:3.9.9-eclipse-temurin-17 AS source

WORKDIR /project

COPY pom.xml .
COPY src ./src


FROM source AS tests

ENTRYPOINT ["mvn", "-B", "-ntp", "test"]


FROM source AS build

RUN mvn -B -ntp -DskipTests package


FROM eclipse-temurin:17-jre AS runtime

WORKDIR /app

COPY --from=build /project/target/chat.jar /app/chat.jar

EXPOSE 5555 6001 6002 9092

ENTRYPOINT ["java", "-jar", "/app/chat.jar"]

CMD ["facade"]