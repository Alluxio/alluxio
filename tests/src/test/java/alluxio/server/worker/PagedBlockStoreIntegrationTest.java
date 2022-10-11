package alluxio.server.worker;

import static org.junit.Assert.assertTrue;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioMaster;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;
import alluxio.worker.block.BlockStoreType;
import alluxio.worker.block.BlockWorker;

import com.google.common.io.ByteStreams;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class PagedBlockStoreIntegrationTest {
  private final String mUfsPath =
      AlluxioTestDirectory.createTemporaryDirectory("ufs").getAbsolutePath();

  @Rule
  public LocalAlluxioClusterResource mLocalCluster = new LocalAlluxioClusterResource.Builder()
      // page store in worker does not support short circuit
      .setProperty(PropertyKey.USER_SHORT_CIRCUIT_ENABLED, false)
      .setProperty(PropertyKey.USER_BLOCK_STORE_TYPE, BlockStoreType.PAGE)
      .setProperty(PropertyKey.USER_CLIENT_CACHE_SIZE, ImmutableList.of(Constants.MB))
      .setProperty(PropertyKey.USER_CLIENT_CACHE_DIRS, ImmutableList.of(
          AlluxioTestDirectory.createTemporaryDirectory("page_store").getAbsolutePath()))
      .setProperty(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE, Constants.KB)
      .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, mUfsPath)
      .build();

  /**
   * Tests that the pages stored in the local page store are preserved across worker restarts.
   */
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.USER_CLIENT_CACHE_STORE_TYPE, "LOCAL",
  })
  @Test
  public void localPageStorePreservesPagesAfterRestart() throws Exception {
    // prepare data in UFS
    try (UnderFileSystem ufs = UnderFileSystem.Factory.createForRoot(Configuration.global());
         OutputStream os = ufs.create(PathUtils.concatPath(mUfsPath, "read-from-ufs"))) {
      os.write(BufferUtils.getIncreasingByteArray(Constants.KB * 2));
    }
    // create a file through alluxio
    final int startOffset = 1;
    LocalAlluxioMaster master = mLocalCluster.get().getLocalAlluxioMaster();
    try (OutputStream os = master.getClient().createFile(
        new AlluxioURI("/write-into-alluxio"),
        CreateFilePOptions.newBuilder().setWriteType(WritePType.MUST_CACHE).build())) {
      os.write(BufferUtils.getIncreasingByteArray(startOffset, Constants.KB));
    }
    // read the file from UFS so that it gets cached in worker storage
    try (InputStream is = master.getClient().openFile(
        new AlluxioURI("/read-from-ufs"),
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE).build())) {
      byte[] content = ByteStreams.toByteArray(is);
      assertTrue(BufferUtils.equalIncreasingByteArray(Constants.KB * 2, content));
    }
    try (InputStream is = master.getClient().openFile(
        new AlluxioURI("/write-into-alluxio"),
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE).build())) {
      byte[] content = ByteStreams.toByteArray(is);
      assertTrue(BufferUtils.equalIncreasingByteArray(startOffset, Constants.KB, content));
    }
    // check the blocks are in the worker
    BlockWorker worker =
        mLocalCluster.get().getWorkerProcess().getWorker(BlockWorker.class);
    for (String path : ImmutableList.of("/read-from-ufs", "/write-into-alluxio")) {
      URIStatus status = master.getClient().getStatus(new AlluxioURI(path));
      List<Long> blocks = status.getBlockIds();
      for (long block : blocks) {
        assertTrue(worker.getBlockStore().hasBlockMeta(block));
      }
    }
    // restart the worker
    mLocalCluster.get().stopWorkers();
    mLocalCluster.get().startWorkers();
    mLocalCluster.get().waitForWorkersRegistered(5000);
    // verify the blocks are still there
    worker = mLocalCluster.get().getWorkerProcess().getWorker(BlockWorker.class);
    for (String path : ImmutableList.of("/read-from-ufs", "/write-into-alluxio")) {
      URIStatus status = master.getClient().getStatus(new AlluxioURI(path));
      List<Long> blocks = status.getBlockIds();
      for (long block : blocks) {
        assertTrue(worker.getBlockStore().hasBlockMeta(block));
      }
    }
  }
}
