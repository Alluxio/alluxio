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

package alluxio.client.resource;

import alluxio.client.block.BlockWorkerClient;
import alluxio.wire.LockBlockResult;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A resource that unlocks the block when it is closed.
 */
@NotThreadSafe
public final class LockBlockResource implements Closeable {
  private final BlockWorkerClient mClient;
  private final LockBlockResult mResult;
  private final long mBlockId;
  private boolean mClosed;

  /**
   * Creates a new instance of {@link LockBlockResource} using the given lock.
   *
   * @param client the block worker client
   * @param result the lock block result
   * @param blockId the block ID
   */
  public LockBlockResource(BlockWorkerClient client, LockBlockResult result, long blockId) {
    mClient = client;
    mResult = result;
    mBlockId = blockId;
  }

  /**
   * @return the lock block result
   */
  public LockBlockResult getResult() {
    return mResult;
  }

  /**
   * Releases the lock.
   */
  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }

    mClosed = true;
    mClient.unlockBlock(mBlockId);
  }
}
