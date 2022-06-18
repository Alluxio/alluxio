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

package alluxio.worker.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import alluxio.ConfigurationRule;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.OpenLocalBlockRequest;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.underfs.UfsManager;
import alluxio.util.io.PathUtils;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockMasterClientPool;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.BlockStoreType;
import alluxio.worker.block.CreateBlockOptions;
import alluxio.worker.block.MonoBlockStore;
import alluxio.worker.block.TieredBlockStore;
import alluxio.worker.block.io.BlockWriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class ShortCircuitBlockReadHandlerTest {
  // templates used in current configuration
  private static final PropertyKey.Template ALIAS_TEMPLATE =
      PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS;
  private static final PropertyKey.Template PATH_TEMPLATE =
      PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH;
  private static final PropertyKey.Template QUOTA_TEMPLATE =
      PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA;
  private static final PropertyKey.Template MEDIUM_TEMPLATE =
      PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_MEDIUMTYPE;

  // test configuration values
  private static final String TIER_0_ALIAS = "ALIAS_0";
  private static final String TIER_1_ALIAS = "ALIAS_1";
  private static final String WORKER_DATA_FOLDER = "TestWorker";
  private static final String WORKER_TMP_FOLDER = "TestTmpWorker";

  @Rule
  public ConfigurationRule mConfiguration = new ConfigurationRule(
      new ImmutableMap.Builder<PropertyKey, Object>()
          // disable paging for this test
          .put(PropertyKey.USER_BLOCK_STORE_TYPE, BlockStoreType.FILE)
          // use 2 tiers to test possible moves of blocks
          .put(PropertyKey.WORKER_TIERED_STORE_LEVELS, 2)
          // disable alignment so that no space needs to be reserved
          // on directories
          .put(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_ENABLED, false)
          .put(PropertyKey.WORKER_DATA_FOLDER, WORKER_DATA_FOLDER)
          .put(PropertyKey.WORKER_DATA_TMP_FOLDER, WORKER_TMP_FOLDER)
          // tier configurations
          .put(ALIAS_TEMPLATE.format(0), TIER_0_ALIAS)
          .put(ALIAS_TEMPLATE.format(1), TIER_1_ALIAS)
          .put(QUOTA_TEMPLATE.format(0), "1024")
          .put(QUOTA_TEMPLATE.format(1), "1024")
          .put(MEDIUM_TEMPLATE.format(0), "MEM")
          .put(MEDIUM_TEMPLATE.format(1), "SSD")
          .build(),
      Configuration.modifiableGlobal()
  );

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  // each tier's storage directory
  private File mTier0Dir;
  private File mTier1Dir;

  // mocked response observer to interact with the handler
  private TestResponseObserver mResponseObserver;

  private final Closer mCloser = Closer.create();
  // local store that backs the short circuit read
  private BlockStore mBlockStore;
  private ShortCircuitBlockReadHandler mTestHandler;

  @Before
  public void before() throws Exception {
    // set up storage tier directories
    mTier0Dir = mFolder.newFolder();
    mTier1Dir = mFolder.newFolder();
    mConfiguration.set(PATH_TEMPLATE.format(0), mTier0Dir.getAbsolutePath());
    mConfiguration.set(PATH_TEMPLATE.format(1), mTier1Dir.getAbsolutePath());

    // set up response observer
    mResponseObserver = new TestResponseObserver();

    // set up local storage
    // todo(yangchen): BlockStore should not worry about the logic of reporting to master,
    // could leave that work to BlockWorker so that we can get rid of these irrelevant mocks
    BlockMasterClientPool pool = mock(BlockMasterClientPool.class);
    BlockMasterClient cli = mock(BlockMasterClient.class);
    doReturn(cli).when(pool).createNewResource();
    doReturn(cli).when(pool).acquire();
    mBlockStore = new MonoBlockStore(new TieredBlockStore(), pool,
        mock(UfsManager.class), new AtomicReference<>(1L));

    mTestHandler = new ShortCircuitBlockReadHandler(mBlockStore, mResponseObserver);
  }

  @Test
  public void accessNonExistentBlock() {
    // we started with empty directories without any block
    long blockId = 1L;
    OpenLocalBlockRequest request = createRequest(blockId, false);
    mTestHandler.onNext(request);

    // check that we get a proper error and no response
    assertTrue(mResponseObserver.getResponses().isEmpty());
    assertFalse(mResponseObserver.isCompleted());
    Throwable t = mResponseObserver.getError();
    assertTrue(t != null && t.getMessage().contains("BlockMeta not found"));
  }

  @Test
  public void accessBlockNoPromote() throws Exception {
    accessBlock(false);
  }

  @Test
  public void accessBlockPromote() throws Exception {
    accessBlock(true);
  }

  @Test
  @Ignore("This test currently takes too long")
  public void accessBlockPinned() throws Exception {
    long blockId = 2L;
    createLocalBlock(
        1L, blockId, 0);

    OpenLocalBlockRequest request = createRequest(blockId, false);
    mTestHandler.onNext(request);

    // block 2 should be pinned now
    // and an attempt to remove the block from local store should fail
    //todo(yangchen): TieredBlockStore's REMOVE_BLOCK_TIMEOUT_MS is too long for testing
    assertThrows(Exception.class, () -> mBlockStore.removeBlock(3L, blockId));
  }

  @Test(timeout = 5000)
  public void unpinBlockOnError() throws Exception {
    long blockId = 2L;
    createLocalBlock(
        1L, blockId, 0);

    OpenLocalBlockRequest request = createRequest(blockId, false);
    mTestHandler.onNext(request);
    mTestHandler.onError(new RuntimeException());

    // block should be unpinned and another session can remove it
    mBlockStore.removeBlock(3L, blockId);
  }

  @Test(timeout = 5000)
  public void errorReAccessingBlock() throws Exception {
    long blockId = 2L;
    createLocalBlock(
        1L, blockId, 0);

    OpenLocalBlockRequest request = createRequest(blockId, false);

    // first call should be ok
    mTestHandler.onNext(request);
    assertNull(mResponseObserver.getError());

    // second call should cause an error
    mTestHandler.onNext(request);

    assertFalse(mResponseObserver.isCompleted());
    assertNotNull(mResponseObserver.getError());

    // the block should be unpinned
    mBlockStore.removeBlock(3L, blockId);
  }

  @After
  public void after() throws Exception {
    mCloser.close();
  }

  private OpenLocalBlockRequest createRequest(long blockId, boolean promote) {
    return OpenLocalBlockRequest.newBuilder().setBlockId(blockId).setPromote(promote).build();
  }

  private void accessBlock(boolean promote) throws Exception {
    // create a block in second tier(tier1) so that promotion will move block
    long sessionId = 1L;
    long blockId = 2L;
    createLocalBlock(sessionId, blockId, 1);

    // access the new block via short circuit read
    OpenLocalBlockRequest request = createRequest(blockId, promote);
    mTestHandler.onNext(request);
    mTestHandler.onCompleted();

    // if promote, the block should be moved to tier 1
    // real block path is <tier_dir_path>/<WORKER_DATA_FOLDER>/<blockId>
    String expectedRootDirPath = (promote ? mTier0Dir : mTier1Dir).getAbsolutePath();
    String expectedBlockPath = PathUtils.concatPath(
        expectedRootDirPath, WORKER_DATA_FOLDER, blockId);

    // check that the block file is present on disk
    assertTrue(Files.exists(Paths.get(expectedBlockPath)));

    // check the response is correct
    assertNull(mResponseObserver.getError());
    assertTrue(mResponseObserver.isCompleted());

    List<OpenLocalBlockResponse> responses = mResponseObserver.getResponses();
    assertEquals(1, responses.size());
    OpenLocalBlockResponse response = responses.get(0);

    // check that we get the expected path where the block lies
    assertEquals(expectedBlockPath, response.getPath());
  }

  // create and commit a block in LocalStorage in the specified location
  // so that further request can read the block via short circuit
  private void createLocalBlock(
      long sessionId, long blockId, int tier) throws Exception {
    mBlockStore.createBlock(
        sessionId,
        blockId,
        tier,
        new CreateBlockOptions(null, null, 64));

    byte[] data = new byte[64];
    Arrays.fill(data, (byte) 1);
    ByteBuffer buf = ByteBuffer.wrap(data);
    BlockWriter writer = mBlockStore.createBlockWriter(sessionId, blockId);
    writer.append(buf);

    mBlockStore.commitBlock(sessionId, blockId, false);
  }

  // a testing response observer that keeps track of the interactions with
  // the handler
  private static class TestResponseObserver implements StreamObserver<OpenLocalBlockResponse> {
    // keeps track of received response
    private final List<OpenLocalBlockResponse> mResponses = new ArrayList<>();
    // keeps track of received error
    private Throwable mError = null;
    // keeps track of completion
    private boolean mCompleted = false;

    @Override
    public void onNext(OpenLocalBlockResponse value) {
      mResponses.add(value);
    }

    @Override
    public void onError(Throwable t) {
      mError = t;
    }

    @Override
    public void onCompleted() {
      mCompleted = true;
    }

    public List<OpenLocalBlockResponse> getResponses() {
      return ImmutableList.copyOf(mResponses);
    }

    public Throwable getError() {
      return mError;
    }

    public boolean isCompleted() {
      return mCompleted;
    }
  }
}
