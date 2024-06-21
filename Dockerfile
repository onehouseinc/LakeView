FROM openjdk:11-jre-slim
WORKDIR /usr/src/app/
ADD /build/libs/lakeView-1.0-SNAPSHOT-all.jar lakeView-1.0-SNAPSHOT-all.jar

RUN useradd -u 1001 onehouse
USER onehouse

ENV FAT_JAR lakeView-1.0-SNAPSHOT-all.jar
ENTRYPOINT ["sh", "-c", "java -XX:MaxRAMPercentage=75.0 -jar $FAT_JAR \"$@\"", "--"]