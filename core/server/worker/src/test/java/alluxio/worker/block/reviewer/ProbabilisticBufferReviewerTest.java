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

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.FormatUtils;
import alluxio.worker.block.meta.StorageDirView;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ProbabilisticBufferReviewerTest {
  private static final long DISK_SIZE = FormatUtils.parseSpaceSize("16GB");
  private static final String SOFT_LIMIT = "256MB";
  private static final long SOFT_LIMIT_BYTES = FormatUtils.parseSpaceSize(SOFT_LIMIT);
  private static final String HARD_LIMIT = "64MB";
  private static final long HARD_LIMIT_BYTES = FormatUtils.parseSpaceSize(HARD_LIMIT);

  private ProbabilisticBufferReviewer mReviewer;

  @Before
  public void createReviewerInstance() {
    ServerConfiguration.set(PropertyKey.WORKER_REVIEWER_CLASS,
            ProbabilisticBufferReviewer.class.getName());
    ServerConfiguration.set(PropertyKey.WORKER_REVIEWER_PROBABILISTIC_HARDLIMIT_BYTES, HARD_LIMIT);
    ServerConfiguration.set(PropertyKey.WORKER_REVIEWER_PROBABILISTIC_SOFTLIMIT_BYTES, SOFT_LIMIT);

    Reviewer reviewer = Reviewer.Factory.create();
    assertTrue(reviewer instanceof ProbabilisticBufferReviewer);
    mReviewer = (ProbabilisticBufferReviewer) reviewer;
  }

  @After
  public void reset() {
    mReviewer = null;
    ServerConfiguration.reset();
  }

  @Test
  public void testProbabilityFunction() throws Exception {
    // Empty - 100%
    StorageDirView mockEmptyDir = mock(StorageDirView.class);
    when(mockEmptyDir.getAvailableBytes()).thenReturn(DISK_SIZE);
    when(mockEmptyDir.getCapacityBytes()).thenReturn(DISK_SIZE);
    double probEmptyDir = mReviewer.getProbability(mockEmptyDir);
    assertEquals(1.0, probEmptyDir, 1e-6);

    // Higher than soft limit - 100%
    StorageDirView mockMoreThanSoft = mock(StorageDirView.class);
    when(mockMoreThanSoft.getAvailableBytes()).thenReturn(SOFT_LIMIT_BYTES + 1);
    when(mockMoreThanSoft.getCapacityBytes()).thenReturn(DISK_SIZE);
    double probMoreThanSoft = mReviewer.getProbability(mockMoreThanSoft);
    assertEquals(1.0, probMoreThanSoft, 1e-6);

    // Lower than soft limit - less than 100%
    StorageDirView mockLessThanSoft = mock(StorageDirView.class);
    when(mockLessThanSoft.getAvailableBytes()).thenReturn(SOFT_LIMIT_BYTES - 1);
    when(mockLessThanSoft.getCapacityBytes()).thenReturn(DISK_SIZE);
    double probLessThanSoft = mReviewer.getProbability(mockLessThanSoft);
    assertEquals(0.99999999, probLessThanSoft, 1e-4);

    // Between soft limit and hard limit - linear
    StorageDirView mockMoreThanHard = mock(StorageDirView.class);
    when(mockMoreThanHard.getAvailableBytes()).thenReturn(FormatUtils.parseSpaceSize("128MB"));
    when(mockMoreThanHard.getCapacityBytes()).thenReturn(DISK_SIZE);
    double probMoreThanHard = mReviewer.getProbability(mockMoreThanHard);
    assertEquals(1.0 / 3, probMoreThanHard, 1e-6);

    // Hard limit reached - 0.0
    StorageDirView mockHardLimit = mock(StorageDirView.class);
    when(mockHardLimit.getAvailableBytes()).thenReturn(HARD_LIMIT_BYTES);
    when(mockHardLimit.getCapacityBytes()).thenReturn(DISK_SIZE);
    double probHardLimit = mReviewer.getProbability(mockHardLimit);
    assertEquals(0.0, probHardLimit, 1e-6);

    // Below hard limit - 0.0
    StorageDirView mockLessThanHard = mock(StorageDirView.class);
    when(mockLessThanHard.getAvailableBytes()).thenReturn(HARD_LIMIT_BYTES - 1);
    when(mockLessThanHard.getCapacityBytes()).thenReturn(DISK_SIZE);
    double probLessThanHard = mReviewer.getProbability(mockLessThanHard);
    assertEquals(0.0, probLessThanHard, 1e-6);

    // Full - 0.0
    StorageDirView mockFull = mock(StorageDirView.class);
    when(mockFull.getAvailableBytes()).thenReturn(0L);
    when(mockFull.getCapacityBytes()).thenReturn(DISK_SIZE);
    double probFull = mReviewer.getProbability(mockFull);
    assertEquals(0.0, probFull, 1e-6);
  }
}
