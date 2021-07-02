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

public class LRUAnnotatorTest extends AbstractBlockAnnotatorTest {
  /**
   * Sets up base class for LRUAnnotator.
   */
  @Before
  public void before() throws Exception {
    ServerConfiguration.set(PropertyKey.WORKER_BLOCK_ANNOTATOR_CLASS,
        LRUAnnotator.class.getName());
    init();
  }

  @Test
  public void testLRU() throws Exception {
    Random rand = new Random();
    List<Long> expectedList = new ArrayList<>();
    for (long i = 0; i < 100; i++) {
      StorageDir pickedDir = getDir(rand.nextInt(2), rand.nextInt(2));
      createBlock(i, pickedDir);
      accessBlock(i);
      expectedList.add(i);
    }

    validateIterator(mBlockIterator.getIterator(BlockStoreLocation.anyTier(), BlockOrder.NATURAL),
        expectedList.iterator());

    Collections.reverse(expectedList);
    validateIterator(mBlockIterator.getIterator(BlockStoreLocation.anyTier(), BlockOrder.REVERSE),
        expectedList.iterator());
  }
}
