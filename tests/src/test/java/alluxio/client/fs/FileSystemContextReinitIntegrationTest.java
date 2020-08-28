/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.fs;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.ConfigHashSync;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemContextReinitializer;
import alluxio.client.meta.MetaMasterConfigClient;
import alluxio.client.meta.RetryHandlingMetaMasterConfigClient;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateFilePOptions;
import alluxio.master.MasterClientContext;
import alluxio.resource.CloseableResource;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Tests reinitializing {@link FileSystemContext}.
 */
public final class FileSystemContextReinitIntegrationTest extends BaseIntegrationTest {
  private static final AlluxioURI PATH_TO_UPDATE = new AlluxioURI("/path/to/update");
  private static final PropertyKey KEY_TO_UPDATE = PropertyKey.USER_FILE_READ_TYPE_DEFAULT;
  private static final String UPDATED_VALUE = ReadType.NO_CACHE.toString();

  private FileSystemContext mContext;
  private String mClusterConfHash;
  private String mPathConfHash;
  private ConfigHashSync mExecutor;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  @Before
  public void before() throws Exception {
    mContext = FileSystemContext.create(ServerConfiguration.global());
    mContext.getClientContext().loadConf(mContext.getMasterAddress(), true, true);
    updateHash();

    FileSystemContextReinitializer reinit = Whitebox.getInternalState(mContext,
        "mReinitializer");
    mExecutor = Whitebox.getInternalState(reinit, "mExecutor");
  }

  @Test
  public void noConfUpdateAndNoRestart() throws Exception {
    triggerAndWaitSync();
    checkHash(false, false);
  }

  @Test
  public void restartWithoutConfUpdate() throws Exception {
    restartMasters();
    triggerAndWaitSync();
    checkHash(false, false);
  }

  @Test
  public void clusterConfUpdate() throws Exception {
    checkClusterConfBeforeUpdate();
    updateClusterConf();
    triggerAndWaitSync();
    checkClusterConfAfterUpdate();
    checkHash(true, false);
  }

  @Test
  public void pathConfUpdate() throws Exception {
    checkPathConfBeforeUpdate();
    updatePathConf();
    triggerAndWaitSync();
    checkPathConfAfterUpdate();
    checkHash(false, true);
  }

  @Test
  public void configHashSync() throws Exception {
    checkClusterConfBeforeUpdate();
    checkPathConfBeforeUpdate();
    updateClusterConf();
    updatePathConf();
    triggerAndWaitSync();
    checkClusterConfAfterUpdate();
    checkPathConfAfterUpdate();
    checkHash(true, true);
    updateHash();
    triggerAndWaitSync();
    checkHash(false, false);
  }

  @Test
  public void blockWorkerClientReinit() throws Exception {
    FileSystemContext fsContext = FileSystemContext.create(ServerConfiguration.global());
    try (CloseableResource<BlockWorkerClient> client =
        fsContext.acquireBlockWorkerClient(mLocalAlluxioClusterResource.get().getWorkerAddress())) {
      fsContext.reinit(true, true);
      fsContext.acquireBlockWorkerClient(mLocalAlluxioClusterResource.get().getWorkerAddress())
          .close();
    }
  }

  @Test
  public void configHashSyncWithOpenStream() throws Exception {
    // Cannot use mLocalAlluxioClusterResource.get().getClient(mContext) here because the clients
    // will be closed when restarting local masters, which in turn will close the contexts.
    try (FileSystem client = FileSystem.Factory.create(mContext);
      FileOutStream os = client.createFile(PATH_TO_UPDATE, CreateFilePOptions.newBuilder()
          .setRecursive(true).build())) {
      checkClusterConfBeforeUpdate();
      checkPathConfBeforeUpdate();
      updateClusterConf();
      updatePathConf();

      ExecutorService service = Executors.newSingleThreadExecutor();
      Future future = service.submit(() -> {
        mExecutor.heartbeat();
      });
      TimeUnit.SECONDS.sleep(1);
      // Stream is open, so reinitialization should block until the stream is closed.
      Assert.assertFalse(future.isDone());
      future.cancel(true);
      checkHash(false, false);
      os.close();
      // Stream is closed, reinitialization should not be blocked.
      triggerAndWaitSync();
      checkClusterConfAfterUpdate();
      checkPathConfAfterUpdate();
      checkHash(true, true);
    }
  }

  /**
   * Triggers ConfigHashSync heartbeat and waits for it to finish.
   */
  private void triggerAndWaitSync() throws Exception {
    mExecutor.heartbeat();
  }

  private void restartMasters() throws Exception {
    mLocalAlluxioClusterResource.get().stopMasters();
    mLocalAlluxioClusterResource.get().startMasters();
  }

  private void updateClusterConf() throws Exception {
    mLocalAlluxioClusterResource.get().stopMasters();
    ServerConfiguration.set(KEY_TO_UPDATE, UPDATED_VALUE);
    mLocalAlluxioClusterResource.get().startMasters();
  }

  private void updatePathConf() throws Exception {
    MetaMasterConfigClient client = new RetryHandlingMetaMasterConfigClient(
        MasterClientContext.newBuilder(mContext.getClientContext()).build());
    client.setPathConfiguration(PATH_TO_UPDATE, KEY_TO_UPDATE, UPDATED_VALUE);
  }

  private void checkClusterConfBeforeUpdate() {
    Assert.assertNotEquals(UPDATED_VALUE, mContext.getClientContext().getClusterConf()
        .get(KEY_TO_UPDATE));
  }

  private void checkClusterConfAfterUpdate() {
    Assert.assertEquals(UPDATED_VALUE, mContext.getClientContext().getClusterConf()
        .get(KEY_TO_UPDATE));
  }

  private void checkPathConfBeforeUpdate() {
    Assert.assertFalse(mContext.getClientContext().getPathConf().getConfiguration(
        PATH_TO_UPDATE, KEY_TO_UPDATE).isPresent());
  }

  private void checkPathConfAfterUpdate() {
    Assert.assertEquals(UPDATED_VALUE, mContext.getClientContext().getPathConf().getConfiguration(
        PATH_TO_UPDATE, KEY_TO_UPDATE).get().get(KEY_TO_UPDATE));
  }

  private void checkHash(boolean clusterConfHashUpdated, boolean pathConfHashUpdated) {
    // Use Equals and NotEquals so that when test fails, the hashes are printed out for comparison.
    if (clusterConfHashUpdated) {
      Assert.assertNotEquals(mClusterConfHash,
          mContext.getClientContext().getClusterConfHash());
    } else {
      Assert.assertEquals(mClusterConfHash,
          mContext.getClientContext().getClusterConfHash());
    }

    if (pathConfHashUpdated) {
      Assert.assertNotEquals(mPathConfHash,
          mContext.getClientContext().getPathConfHash());
    } else {
      Assert.assertEquals(mPathConfHash,
          mContext.getClientContext().getPathConfHash());
    }
  }

  private void updateHash() {
    mClusterConfHash = mContext.getClientContext().getClusterConfHash();
    mPathConfHash = mContext.getClientContext().getPathConfHash();
  }
}
