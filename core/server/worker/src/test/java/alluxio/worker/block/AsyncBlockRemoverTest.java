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

package alluxio.worker.block;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;

import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Tests for AsyncBlockRemover.
 */
public class AsyncBlockRemoverTest {
  private AsyncBlockRemover mRemover;
  private final BlockingQueue<Long> mBlocksToRemove = new LinkedBlockingQueue<>();
  private final Set<Long> mBlocksTriedToRemoved =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final BlockWorker mMockWorker = Mockito.mock(BlockWorker.class);

  @Test
  public void blockRemove() throws Exception {
    doAnswer(args -> {
      // keeps track the blocks to remove
      mBlocksTriedToRemoved.add(args.getArgument(1));
      return null;
    }).when(mMockWorker).removeBlock(anyLong(), anyLong());
    mRemover = new AsyncBlockRemover(mMockWorker, 10, mBlocksToRemove,
        Collections.newSetFromMap(new ConcurrentHashMap<>()));
    List<Long> blocks = new ArrayList<>();
    for (long i = 0; i < 100; i++) {
      blocks.add(i);
    }
    mRemover.addBlocksToDelete(blocks);
    CommonUtils.waitFor("async block removal completed",
        () -> mBlocksTriedToRemoved.size() == blocks.size(),
        WaitForOptions.defaults().setTimeoutMs(10000));
    assertEquals(0, mBlocksToRemove.size());
  }

  @Test
  public void failedBlockRemove() throws Exception {
    doAnswer(args -> {
      // keeps track the blocks to remove
      mBlocksTriedToRemoved.add(args.getArgument(1));
      // throw exception
      throw new IOException("Failed to remove block");
    }).when(mMockWorker).removeBlock(anyLong(), anyLong());
    mRemover = new AsyncBlockRemover(mMockWorker, 10, mBlocksToRemove,
        Collections.newSetFromMap(new ConcurrentHashMap<>()));
    List<Long> blocks = new ArrayList<>();
    for (long i = 0; i < 100; i++) {
      blocks.add(i);
    }
    mRemover.addBlocksToDelete(blocks);
    CommonUtils.waitFor("async block removal completed",
        () -> mBlocksTriedToRemoved.size() == blocks.size(),
        WaitForOptions.defaults().setTimeoutMs(10000));
    assertEquals(0, mBlocksToRemove.size());
  }
}
