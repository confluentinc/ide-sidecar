FROM maven:{{ mavenVersion }}-openjdk-{{ javaVersion }}-slim AS build
WORKDIR /{{ connectorName }}
COPY src ./src
COPY pom.xml .
RUN mvn -f pom.xml clean package

FROM openjdk:{{ javaVersion }}-slim
WORKDIR /{{ connectorName }}
COPY --from=build /{{ connectorName }}/target/*.jar {{ connectorName }}.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","{{ connectorName }}.jar"]
