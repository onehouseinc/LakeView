package ai.onehouse.lakeview.sync;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class LakeviewGlueSyncToolTest {
  private static final String BASE_PATH = "/tmp/test";

  private Configuration hadoopConf;

  @BeforeEach
  public void setUp() {
    FileSystem fileSystem = HadoopFSUtils.getFs(BASE_PATH, new Configuration());
    hadoopConf = fileSystem.getConf();
  }

  @Test
  void testLakeViewGlueSyncTool() {
    Properties properties = new Properties();
    try (MockedConstruction<LakeviewSyncTool> lakeviewSyncToolMockedConstruction = mockConstruction(LakeviewSyncTool.class,
        (lakeviewSyncTool, context) -> {
          List<?> arguments = context.arguments();
          assertEquals(2, arguments.size());
          assertEquals(properties, arguments.get(0));
          assertEquals(hadoopConf, arguments.get(1));
        }); MockedConstruction<AwsGlueCatalogSyncTool> glueSyncToolMockedConstruction = mockConstruction(AwsGlueCatalogSyncTool.class,
        (glueSyncTool, context) -> {
          List<?> arguments = context.arguments();
          assertEquals(2, arguments.size());
          assertEquals(properties, arguments.get(0));
          assertEquals(hadoopConf, arguments.get(1));
        });
         LakeviewGlueSyncTool lakeviewGlueSyncTool = new LakeviewGlueSyncTool(properties, hadoopConf)) {
      lakeviewGlueSyncTool.syncHoodieTable();

      List<AwsGlueCatalogSyncTool> glueSyncToolsConstructed = glueSyncToolMockedConstruction.constructed();
      assertEquals(1, glueSyncToolsConstructed.size());
      AwsGlueCatalogSyncTool awsGlueCatalogSyncTool = glueSyncToolsConstructed.get(0);
      verify(awsGlueCatalogSyncTool, times(1)).syncHoodieTable();


      List<LakeviewSyncTool> lakeviewSyncToolsConstructed = lakeviewSyncToolMockedConstruction.constructed();
      assertEquals(1, lakeviewSyncToolsConstructed.size());
      LakeviewSyncTool lakeviewSyncTool = lakeviewSyncToolsConstructed.get(0);
      verify(lakeviewSyncTool, times(1)).syncHoodieTable();
    }
  }
}