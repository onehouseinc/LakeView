package ai.onehouse.lakeview.sync;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.hive.HiveSyncTool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class LakeviewHiveSyncToolTest {

  private static final String BASE_PATH = "/tmp/test";

  private Configuration hadoopConf;

  @BeforeEach
  public void setUp() throws IOException {
    FileSystem fileSystem = FileSystem.get(URI.create("file://" + BASE_PATH), new Configuration());
    hadoopConf = fileSystem.getConf();
  }

  @Test
  void testLakeViewHiveSyncTool() {
    Properties properties = new Properties();
    try (MockedConstruction<LakeviewSyncTool> lakeviewSyncToolMockedConstruction = mockConstruction(LakeviewSyncTool.class,
        (lakeviewSyncTool, context) -> {
          List<?> arguments = context.arguments();
          assertEquals(2, arguments.size());
          assertEquals(properties, arguments.get(0));
          assertEquals(hadoopConf, arguments.get(1));
        }); MockedConstruction<HiveSyncTool> hiveSyncToolMockedConstruction = mockConstruction(HiveSyncTool.class,
        (hiveSyncTool, context) -> {
          List<?> arguments = context.arguments();
          assertEquals(2, arguments.size());
          assertEquals(properties, arguments.get(0));
          assertEquals(hadoopConf, arguments.get(1));
        });
         LakeviewHiveSyncTool lakeviewHiveSyncTool = new LakeviewHiveSyncTool(properties, hadoopConf)) {
      lakeviewHiveSyncTool.syncHoodieTable();

      List<HiveSyncTool> hiveSyncToolsConstructed = hiveSyncToolMockedConstruction.constructed();
      assertEquals(1, hiveSyncToolsConstructed.size());
      HiveSyncTool hiveSyncTool = hiveSyncToolsConstructed.get(0);
      verify(hiveSyncTool, times(1)).syncHoodieTable();


      List<LakeviewSyncTool> lakeviewSyncToolsConstructed = lakeviewSyncToolMockedConstruction.constructed();
      assertEquals(1, lakeviewSyncToolsConstructed.size());
      LakeviewSyncTool lakeviewSyncTool = lakeviewSyncToolsConstructed.get(0);
      verify(lakeviewSyncTool, times(1)).syncHoodieTable();
    }
  }
}