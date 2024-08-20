FROM openjdk:11-jre-slim
WORKDIR /usr/src/app/
ADD /build/libs/LakeView-1.0-SNAPSHOT-all.jar LakeView-1.0-SNAPSHOT-all.jar

RUN useradd -u 1001 onehouse
USER onehouse

ENV FAT_JAR LakeView-1.0-SNAPSHOT-all.jar
ENTRYPOINT ["sh", "-c", "java \
  -XX:+UseG1GC \
  -XX:MaxRAMPercentage=75.0 \
  -XX:InitiatingHeapOccupancyPercent=45 \
  -XX:G1ReservePercent=15 \
  -XX:SurvivorRatio=6 \
  -XX:+AlwaysPreTouch \
  -XX:+UseStringDeduplication \
  -XX:+UseCompressedOops \
  -XX:+ParallelRefProcEnabled \
  -XX:G1HeapRegionSize=16M \
  -XX:+UseNUMA \
  -Xlog:gc*:stdout:time,uptime,level,tags \
  -jar $FAT_JAR \"$@\"", "--"]