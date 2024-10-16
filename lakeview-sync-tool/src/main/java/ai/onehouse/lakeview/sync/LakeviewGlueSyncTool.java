package ai.onehouse.lakeview.sync;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool;
import org.apache.hudi.sync.common.HoodieSyncTool;

import java.util.Properties;

public class LakeviewGlueSyncTool extends HoodieSyncTool implements AutoCloseable {

  private final AwsGlueCatalogSyncTool awsGlueCatalogSyncTool;
  private final LakeviewSyncTool lakeviewSyncTool;

  public LakeviewGlueSyncTool(Properties props, Configuration hadoopConf) {
    super(props, hadoopConf);
    this.awsGlueCatalogSyncTool = new AwsGlueCatalogSyncTool(props, hadoopConf);
    this.lakeviewSyncTool = new LakeviewSyncTool(props, hadoopConf);
  }

  @Override
  public void syncHoodieTable() {
    // sync with glue
    awsGlueCatalogSyncTool.syncHoodieTable();
    // perform syncing with lakeview as well
    lakeviewSyncTool.syncHoodieTable();
  }

  @Override
  public void close() {
    awsGlueCatalogSyncTool.close();
    lakeviewSyncTool.close();
  }
}
