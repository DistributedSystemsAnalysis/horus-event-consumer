FROM openjdk:8-jre-slim

ARG DISTRO_NAME=horus-processor
ARG JAR_FILE=horus-consumer-1.0-SNAPSHOT.jar

COPY target/lib lib
COPY target/conf conf
COPY target/kafka-consumer*.jar $JAR_FILE

VOLUME ["/conf"]

CMD ["java", "-jar", "horus-consumer-1.0-SNAPSHOT.jar"]
