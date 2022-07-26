package alluxio.client.cli.fsadmin.command;

import alluxio.AlluxioURI;
import alluxio.client.cli.fsadmin.AbstractFsAdminShellTest;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.WritePType;
import alluxio.util.SleepUtils;
import alluxio.wire.WorkerNetAddress;

import org.junit.Assert;
import org.junit.Test;

public class FreeSpaceCommandIntegrationTest extends AbstractFsAdminShellTest {
  @Test
  public void workerNotRunning() throws Exception {
    WorkerNetAddress workerAddress = mLocalAlluxioCluster.getWorkerAddress();

    mLocalAlluxioCluster.stopWorkers();
    int ret = mFsAdminShell.run(
        "freeSpace", workerAddress.getHost() + ":" + workerAddress.getRpcPort());
    Assert.assertNotEquals(0, ret);
  }

  @Test
  public void freeSpaceWhenWorkerIsEmpty() {
    WorkerNetAddress workerAddress = mLocalAlluxioCluster.getWorkerAddress();
    int ret = mFsAdminShell.run(
        "freeSpace", workerAddress.getHost() + ":" + workerAddress.getRpcPort());
    Assert.assertEquals(0, ret);
  }

  @Test
  public void freeSpaceWhenWorkerHaveEnoughSpace() throws Exception {
    WorkerNetAddress workerAddress = mLocalAlluxioCluster.getWorkerAddress();
    String fileName = "/testFile";
    FileSystem client = mLocalAlluxioCluster.getClient();
    FileSystemTestUtils.createByteFile(client, fileName, WritePType.CACHE_THROUGH, 10);
    int ret = mFsAdminShell.run(
        "freeSpace", "--percent", "1", workerAddress.getHost() + ":" + workerAddress.getRpcPort());
    Assert.assertEquals(0, ret);
    // wait for block heartbeat
    SleepUtils.sleepMs(Configuration.global().getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS));
    Assert.assertEquals(100, client.getStatus(new AlluxioURI(fileName)).getInAlluxioPercentage());
  }

  @Test
  public void freeSpaceWhenWorkerHaveNoEnoughSpace() throws Exception {
    WorkerNetAddress workerAddress = mLocalAlluxioCluster.getWorkerAddress();
    String fileName = "/testFile";
    FileSystem client = mLocalAlluxioCluster.getClient();
    FileSystemTestUtils.createByteFile(client, fileName, WritePType.CACHE_THROUGH, 10);
    int ret = mFsAdminShell.run(
        "freeSpace", workerAddress.getHost() + ":" + workerAddress.getRpcPort());
    Assert.assertEquals(0, ret);
    // wait for block heartbeat
    SleepUtils.sleepMs(Configuration.global().getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS));
    Assert.assertEquals(0, client.getStatus(new AlluxioURI(fileName)).getInAlluxioPercentage());
  }
}
