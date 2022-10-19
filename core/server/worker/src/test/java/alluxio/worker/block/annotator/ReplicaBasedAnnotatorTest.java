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

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.meta.StorageDir;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ReplicaBasedAnnotatorTest extends AbstractBlockAnnotatorTest {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaBasedAnnotatorTest.class);

  /**
  * Sets up base class for ReplicaBasedAnnotator.
  */
  @Before
  public void before() throws Exception {
    Configuration.set(PropertyKey.WORKER_BLOCK_ANNOTATOR_CLASS,
            ReplicaBasedAnnotator.class.getName());
    Configuration.set(PropertyKey.WORKER_BLOCK_ANNOTATOR_REPLICA_LRU_RATIO, 0.00001);
    Configuration.set(PropertyKey.WORKER_BLOCK_ANNOTATOR_REPLICA_REPLICA_RATIO, 1000);
    init();
  }

  @Test
  public void testReplicaBased() throws Exception {
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
    Map<Long, Long> replicaInfo = new HashMap<>();
    expectedList.clear();
    for (long i = 0; i < 100; i += 2) {
      replicaInfo.put(i, 2L);
      expectedList.add(i);
    }
    for (long i = 1; i < 100; i += 2) {
      expectedList.add(i);
    }
    for (long i = 0; i < 100; i++) {
      accessBlock(i);
    }

    updateReplicaInfo(replicaInfo);
    validateIterator(mBlockIterator.getIterator(BlockStoreLocation.anyTier(), BlockOrder.NATURAL),
            expectedList.iterator());

    replicaInfo.clear();
    expectedList.clear();
    for (long i = 0; i < 100; i += 4) {
      replicaInfo.put(i, -1L);
    }
    for (long i = 2; i < 100; i += 4) {
      expectedList.add(i);
    }
    for (long i = 0; i < 100; i += 4) {
      expectedList.add(i);
    }
    for (long i = 1; i < 100; i += 2) {
      expectedList.add(i);
    }
    updateReplicaInfo(replicaInfo);
    for (long i = 0; i < 100; i++) {
      accessBlock(i);
    }
    validateIterator(mBlockIterator.getIterator(BlockStoreLocation.anyTier(), BlockOrder.NATURAL),
            expectedList.iterator());
  }
}
