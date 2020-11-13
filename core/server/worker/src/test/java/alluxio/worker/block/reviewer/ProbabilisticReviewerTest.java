package alluxio.worker.block.reviewer;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.FormatUtils;
import alluxio.worker.block.allocator.MaxFreeAllocator;
import alluxio.worker.block.meta.StorageDirView;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProbabilisticReviewerTest {
  @Test
  public void testProbability() throws Exception {
    ServerConfiguration.set(PropertyKey.WORKER_REVIEWER_CLASS, ProbabilisticReviewer.class.getName());
    ServerConfiguration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "64MB");
    ServerConfiguration.set(PropertyKey.WORKER_TIER_CUTOFF_BYTES, "256MB");

    Reviewer reviewer = Reviewer.Factory.create();
    assertTrue(reviewer instanceof ProbabilisticReviewer);
    ProbabilisticReviewer probReviewer = (ProbabilisticReviewer) reviewer;
    // Empty - 100%
    StorageDirView mockEmptyDir = mock(StorageDirView.class);
    when(mockEmptyDir.getAvailableBytes()).thenReturn(FormatUtils.parseSpaceSize("16GB"));
    when(mockEmptyDir.getCapacityBytes()).thenReturn(FormatUtils.parseSpaceSize("16GB"));
    double probEmptyDir = probReviewer.getProbability(mockEmptyDir);
    assertEquals(1.0, probEmptyDir, 1e-6);

    // Higher than cutoff - 100%
    StorageDirView mockHalfFull = mock(StorageDirView.class);
    when(mockHalfFull.getAvailableBytes()).thenReturn(FormatUtils.parseSpaceSize("256MB") + 1);
    when(mockHalfFull.getCapacityBytes()).thenReturn(FormatUtils.parseSpaceSize("16GB"));
    double probHalfFull = probReviewer.getProbability(mockHalfFull);
    assertEquals(1.0, probHalfFull, 1e-6);

    // Lower than cutoff - less than 100%
    StorageDirView mockMostFull = mock(StorageDirView.class);
    when(mockMostFull.getAvailableBytes()).thenReturn(FormatUtils.parseSpaceSize("256MB") - 1);
    when(mockMostFull.getCapacityBytes()).thenReturn(FormatUtils.parseSpaceSize("16GB"));
    double probMostFull = probReviewer.getProbability(mockMostFull);
    System.out.println("Mostly full for 256MB-1: " + probMostFull);
    assertEquals(0.99999999, probMostFull, 1e-4);

    //
    StorageDirView mockMostFull2 = mock(StorageDirView.class);
    when(mockMostFull2.getAvailableBytes()).thenReturn(FormatUtils.parseSpaceSize("128MB"));
    when(mockMostFull2.getCapacityBytes()).thenReturn(FormatUtils.parseSpaceSize("16GB"));
    double probMostFull2 = probReviewer.getProbability(mockMostFull2);
    System.out.println("Mostly full for 128MB: " + probMostFull2);
    assertEquals(1.0 / 3, probMostFull2, 1e-6);

    StorageDirView mockStopHere = mock(StorageDirView.class);
    when(mockStopHere.getAvailableBytes()).thenReturn(FormatUtils.parseSpaceSize("64MB"));
    when(mockStopHere.getCapacityBytes()).thenReturn(FormatUtils.parseSpaceSize("16GB"));
    double probStopHere = probReviewer.getProbability(mockStopHere);
    System.out.println("Cutoff for 64MB: " + probStopHere);
    assertEquals(0.0, probStopHere, 1e-6);

    StorageDirView mockStopped = mock(StorageDirView.class);
    when(mockStopped.getAvailableBytes()).thenReturn(FormatUtils.parseSpaceSize("64MB") - 1);
    when(mockStopped.getCapacityBytes()).thenReturn(FormatUtils.parseSpaceSize("16GB"));
    double probStopped = probReviewer.getProbability(mockStopped);
    System.out.println("Less than 64MB: " + probStopped);
    assertEquals(0.0, probStopped, 1e-6);
  }
}
