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

package alluxio.worker.block.reviewer;

import static org.junit.Assert.assertTrue;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.Test;

/**
 * Test {@link Reviewer.Factory} by passing different allocate strategy class names with alluxio
 * conf and test if it generates the correct {@link Reviewer} instance.
 * */
public class ReviewerFactoryTest {
  @Test
  public void createProbabilisticBufferReviewer() {
    ServerConfiguration.set(PropertyKey.WORKER_REVIEWER_CLASS,
            ProbabilisticBufferReviewer.class.getName());
    Reviewer allocator = Reviewer.Factory.create();
    assertTrue(allocator instanceof ProbabilisticBufferReviewer);
  }

  /**
   * Tests the creation of the default reviewer via the
   * {@link Reviewer.Factory#create()} method.
   */
  @Test
  public void createDefaultAllocator() {
    // Create a new instance of Alluxio configuration with original properties to test the default
    // behavior of create.
    Reviewer allocator = Reviewer.Factory.create();
    assertTrue(allocator instanceof ProbabilisticBufferReviewer);
  }
}
