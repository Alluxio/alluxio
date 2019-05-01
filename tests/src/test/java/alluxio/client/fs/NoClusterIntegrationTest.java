package alluxio.client.fs;

import alluxio.AlluxioURI;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.LocalAlluxioJobCluster;

import org.junit.Test;

public class NoClusterIntegrationTest {

  @Test
  public void dumbtestNoCluster() throws Exception {
    int sleepSecs = 25;

    InstancedConfiguration conf = ConfigurationTestUtils.defaults();
    conf.set(PropertyKey.USER_METRICS_HEARTBEAT_INTERVAL_MS, "3s");
    conf.set(PropertyKey.USER_METRICS_COLLECTION_ENABLED, "true");
    FileSystem fs = FileSystem.Factory.create(conf);
    int count = 0;
    while (count < sleepSecs) {
      try {
        fs.openFile(new AlluxioURI(("/dir")));
      } catch (Exception e) {
        return;
      }
      Thread.sleep(1000);
      count++;
    }
    Thread.sleep(Constants.SECOND * sleepSecs);
  }

  @Test
  public void dumbtest() throws Exception {
    LocalAlluxioCluster alluxio = new LocalAlluxioCluster();
    alluxio.initConfiguration();
    alluxio.start();
    LocalAlluxioJobCluster jobService = new LocalAlluxioJobCluster();
    jobService.start();

    int sleepSecs = 25;

    InstancedConfiguration conf = ConfigurationTestUtils.defaults();
    conf.set(PropertyKey.USER_METRICS_HEARTBEAT_INTERVAL_MS, "1s");
    conf.set(PropertyKey.USER_METRICS_COLLECTION_ENABLED, "true");
    FileSystem fs = FileSystem.Factory.create(conf);
    int count = 0;
    while (true) {
      FileOutStream fos = fs.createFile(new AlluxioURI(String.format("/%d", count)));
      for (int i = 0; i < sleepSecs; i++) {
        fos.write((byte) i);
        Thread.sleep(500);
      }
      count++;
      Thread.sleep(Constants.SECOND * sleepSecs);
    }
  }
}
