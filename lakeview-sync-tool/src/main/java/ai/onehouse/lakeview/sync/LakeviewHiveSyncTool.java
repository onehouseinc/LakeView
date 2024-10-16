package ai.onehouse.lakeview.sync;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.sync.common.HoodieSyncTool;

import java.util.Properties;

public class LakeviewHiveSyncTool extends HoodieSyncTool implements AutoCloseable {

  private final LakeviewSyncTool lakeviewSyncTool;
  private final HiveSyncTool hiveSyncTool;

  public LakeviewHiveSyncTool(Properties props, Configuration hadoopConf) {
    super(props, hadoopConf);
    this.hiveSyncTool = new HiveSyncTool(props, hadoopConf);
    this.lakeviewSyncTool = new LakeviewSyncTool(props, hadoopConf);
  }

  public void syncHoodieTable() {
    hiveSyncTool.syncHoodieTable();
    // perform syncing with lakeview as well
    lakeviewSyncTool.syncHoodieTable();
  }

  @Override
  public void close() {
    hiveSyncTool.close();
    lakeviewSyncTool.close();
  }
}
