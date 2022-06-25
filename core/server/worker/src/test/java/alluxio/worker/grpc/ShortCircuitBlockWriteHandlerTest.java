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
import static org.junit.Assert.assertTrue;

import alluxio.grpc.CreateLocalBlockRequest;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.util.io.PathUtils;
import alluxio.worker.block.CreateBlockOptions;
import alluxio.worker.block.NoopBlockWorker;

import com.amazonaws.annotation.NotThreadSafe;
import com.google.common.collect.ImmutableList;
import io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ShortCircuitBlockWriteHandlerTest {

  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  private TestResponseObserver mResponseObserver;
  private TestBlockWorker mBlockWorker;
  private ShortCircuitBlockWriteHandler mHandler;

  @Before
  public void before() throws Exception {
    File tmpDir = mTempFolder.newFolder();
    mBlockWorker = new TestBlockWorker(tmpDir.getAbsolutePath());

    mResponseObserver = new TestResponseObserver();

    mHandler = new ShortCircuitBlockWriteHandler(mBlockWorker, mResponseObserver);
  }

  @Test
  public void createBlock() {
    long blockId = 1L;
    CreateLocalBlockRequest request = createRequest(blockId, false);
    // request to create a new local block
    mHandler.onNext(request);

    // verify that we get one response from the handler
    // that contains the right path to the local block file
    assertEquals(1, mResponseObserver.getResponses().size());
    assertNull(mResponseObserver.getError());
    assertFalse(mResponseObserver.ismCompleted());

    String path = mResponseObserver.getResponses().get(0).getPath();
    // verify that the local file exists
    assertTrue(Files.exists(Paths.get(path)));

    // verify that the blockId is recorded into the temp blocks
    // but is not publicly available yet
    assertTrue(mBlockWorker.isTempBlockCreated(blockId));
    assertFalse(mBlockWorker.isBlockCommitted(blockId));

    mHandler.onCompleted();

    // verify that the block is committed and publicly available
    assertTrue(mBlockWorker.isBlockCommitted(blockId));
  }

  @Test
  public void nextRequestWithoutCleaningUp() {
    long blockId = 1L;
    CreateLocalBlockRequest request = createRequest(blockId, false);
    mHandler.onNext(request);
    long anotherBlockId = 2L;
    CreateLocalBlockRequest anotherRequest = createRequest(anotherBlockId, false);
    // this request should fail and abort the previous session
    mHandler.onNext(anotherRequest);

    assertNotNull(mResponseObserver.getError());
    assertFalse(mResponseObserver.ismCompleted());
    assertFalse(mBlockWorker.isTempBlockCreated(blockId));
    assertFalse(mBlockWorker.isTempBlockCreated(anotherBlockId));
  }

  @Test
  public void requestSpaceForNonExistingBlock() {
    long blockId = 1L;
    CreateLocalBlockRequest request = createRequest(blockId, true);
    // this request should fail as the block
    // is not created yet
    mHandler.onNext(request);

    assertTrue(mResponseObserver.getResponses().isEmpty());
    assertFalse(mResponseObserver.ismCompleted());
    Throwable t = mResponseObserver.getError();
    assertTrue(
        t != null
        && t.getMessage().contains("does not exists"));
  }

  @Test
  public void abortBlockOnCancel() {
    long blockId = 1L;
    CreateLocalBlockRequest request = createRequest(blockId, false);
    mHandler.onNext(request);
    mHandler.onCancel();

    // block should be aborted
    assertFalse(mBlockWorker.isBlockCommitted(blockId));
    assertFalse(mBlockWorker.isTempBlockCreated(blockId));
  }

  @Test
  public void abortBlockOnError() {
    long blockId = 1L;
    CreateLocalBlockRequest request = createRequest(blockId, false);
    mHandler.onNext(request);

    // now the temp block is created
    assertTrue(mBlockWorker.isTempBlockCreated(blockId));

    mHandler.onError(new RuntimeException());

    // now the block should be aborted and
    // the session cleaned up
    assertFalse(mBlockWorker.isTempBlockCreated(blockId));
    assertFalse(mBlockWorker.isBlockCommitted(blockId));

    // verify that we get the correct response
    assertFalse(mResponseObserver.ismCompleted());
    assertNotNull(mResponseObserver.getError());
  }

  private CreateLocalBlockRequest createRequest(long blockId, boolean onlyReserveSpace) {
    return CreateLocalBlockRequest
        .newBuilder()
        .setBlockId(blockId)
        .setOnlyReserveSpace(onlyReserveSpace)
        .build();
  }

  private static final class TestResponseObserver
      implements StreamObserver<CreateLocalBlockResponse> {

    private final List<CreateLocalBlockResponse> mReceivedResponses = new ArrayList<>();
    private Throwable mError = null;
    private boolean mCompleted = false;

    @Override
    public void onNext(CreateLocalBlockResponse value) {
      mReceivedResponses.add(value);
    }

    @Override
    public void onError(Throwable t) {
      mError = t;
    }

    @Override
    public void onCompleted() {
      mCompleted = true;
    }

    public List<CreateLocalBlockResponse> getResponses() {
      return ImmutableList.copyOf(mReceivedResponses);
    }

    public Throwable getError() {
      return mError;
    }

    public boolean ismCompleted() {
      return mCompleted;
    }
  }

  /**
   * A test block worker that interacts with ShortCircuitBlockWriteHandler.
   * It does the minimum necessary book-keeping and file I/O to make short circuit
   * write possible.
   */
  @NotThreadSafe
  private static final class TestBlockWorker extends NoopBlockWorker {
    // path to the working directory of
    // this worker
    private final String mRootDirPath;

    // keeps track of
    // temporary block -> owning session
    private Map<Long, Long> mTempBlocks;
    // public blocks
    // an invariant is maintained that no block can be in both mTempBlocks and
    // mPublicBlocks
    private List<Long> mPublicBlocks;

    private TestBlockWorker(String rootDirectory) {
      mRootDirPath = rootDirectory;
      mTempBlocks = new HashMap<>();
      mPublicBlocks = new ArrayList<>();
    }

    @Override
    public void abortBlock(long sessionId, long blockId) {
      if (mTempBlocks.containsKey(blockId) && mTempBlocks.get(blockId).equals(sessionId)) {
        mTempBlocks.remove(blockId);
      }
    }

    @Override
    public void commitBlock(long sessionId, long blockId, boolean pinOnCreate) {
      if (mTempBlocks.containsKey(blockId) && mTempBlocks.get(blockId).equals(sessionId)) {
        mTempBlocks.remove(blockId);
        mPublicBlocks.add(blockId);
        return;
      }
      throw new RuntimeException(
          String.format("Block %d does not exists for session %d", blockId, sessionId));
    }

    @Override
    public String createBlock(long sessionId, long blockId, int tier,
                              CreateBlockOptions createBlockOptions) throws IOException {
      if (mTempBlocks.containsKey(blockId) && mPublicBlocks.contains(blockId)) {
        throw new RuntimeException(String.format("Block %d already exists", blockId));
      }
      mTempBlocks.put(blockId, sessionId);
      String filePath = getPath(blockId);
      // create file
      File f = new File(filePath);
      if (!f.createNewFile()) {
        throw new IOException("Block File Already Exists");
      }
      return filePath;
    }

    @Override
    public void requestSpace(long sessionId, long blockId, long additionalBytes) {
      if (mTempBlocks.containsKey(blockId) && mTempBlocks.get(blockId).equals(sessionId)) {
        // no need to do anything for this class
        return;
      }
      throw new RuntimeException(
          String.format("Block %d does not exists for session %d", blockId, sessionId));
    }

    @Override
    public void cleanupSession(long sessionId) {
      // remove all temp blocks owned by the session
      mTempBlocks = mTempBlocks
          .entrySet()
          .stream()
          .filter(entry -> !entry.getValue().equals(sessionId))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public boolean isTempBlockCreated(long blockId) {
      return mTempBlocks.containsKey(blockId);
    }

    public boolean isBlockCommitted(long blockId) {
      return mPublicBlocks.contains(blockId);
    }

    // a block's file path is <root_dir_path>/<blockId>
    private String getPath(long blockId) {
      return PathUtils.concatPath(mRootDirPath, blockId);
    }
  }
}
