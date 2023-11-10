FROM openjdk:11-jre-slim
WORKDIR /usr/src/app/
ADD /build/libs/HudiMetadataExtractor-1.0-SNAPSHOT-all.jar HudiMetadataExtractor-1.0-SNAPSHOT-all.jar

# Run
ENV FAT_JAR HudiMetadataExtractor-1.0-SNAPSHOT-all.jar
ENTRYPOINT ["sh", "-c", "java -jar $FAT_JAR \"$@\"", "--"]