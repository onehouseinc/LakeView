FROM openjdk:11-jre-slim
WORKDIR /usr/src/app/
COPY /build/libs/LakeView-1.0-SNAPSHOT-all.jar LakeView-1.0-SNAPSHOT-all.jar

RUN useradd -u 1001 onehouse
USER onehouse

ENV FAT_JAR LakeView-1.0-SNAPSHOT-all.jar
ENTRYPOINT ["sh", "-c", "java -XX:MaxRAMPercentage=75.0 -jar $FAT_JAR \"$@\"", "--"]
