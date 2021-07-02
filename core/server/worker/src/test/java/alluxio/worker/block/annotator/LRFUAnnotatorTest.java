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

package alluxio.worker.block.annotator;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.meta.StorageDir;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class LRFUAnnotatorTest extends AbstractBlockAnnotatorTest {
  /**
   * Sets up base class for LRUAnnotator.
   */
  @Before
  public void before() throws Exception {
    ServerConfiguration.set(PropertyKey.WORKER_BLOCK_ANNOTATOR_CLASS,
        LRFUAnnotator.class.getName());
    // To make it behave close to an absolute LFU.
    ServerConfiguration.set(PropertyKey.WORKER_BLOCK_ANNOTATOR_LRFU_STEP_FACTOR, 0);
    init();
  }

  @Test
  public void testLRFU() throws Exception {
    Random rand = new Random();
    List<Long> expectedList = new ArrayList<>();
    for (long i = 0; i < 100; i++) {
      StorageDir pickedDir = getDir(rand.nextInt(2), rand.nextInt(2));
      createBlock(i, pickedDir);
      for (int j = 0; j < i * 2 + 1; j++) {
        accessBlock(i);
      }
      expectedList.add(i);
    }

    validateIterator(mBlockIterator.getIterator(BlockStoreLocation.anyTier(), BlockOrder.NATURAL),
        expectedList.iterator());

    Collections.reverse(expectedList);
    validateIterator(mBlockIterator.getIterator(BlockStoreLocation.anyTier(), BlockOrder.REVERSE),
        expectedList.iterator());
  }
}
