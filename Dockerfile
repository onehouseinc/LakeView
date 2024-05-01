FROM openjdk:11-jre-slim
WORKDIR /usr/src/app/
ADD /build/libs/HudiMetadataExtractor-1.0-SNAPSHOT-all.jar HudiMetadataExtractor-1.0-SNAPSHOT-all.jar

LABEL "org.opencontainers.image.base.name"="Onehouse"

RUN useradd -u 1001 onehouse
USER onehouse

ENV FAT_JAR HudiMetadataExtractor-1.0-SNAPSHOT-all.jar
ENTRYPOINT ["sh", "-c", "java -XX:MaxRAMPercentage=60.0 -jar $FAT_JAR \"$@\"", "--"]