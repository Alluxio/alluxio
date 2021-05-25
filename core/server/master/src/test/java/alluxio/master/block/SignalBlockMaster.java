package alluxio.master.block;

import alluxio.clock.ManualClock;
import alluxio.master.CoreMasterContext;
import alluxio.master.metrics.MetricsMaster;
import alluxio.resource.LockResource;
import alluxio.util.executor.ExecutorServiceFactory;

import java.util.concurrent.CountDownLatch;

/**
 * When the writer is writing, issue a signal so the reader will start reading
 * */
class SignalBlockMaster extends DefaultBlockMaster {
  CountDownLatch mLatch;

  SignalBlockMaster(MetricsMaster metricsMaster, CoreMasterContext masterContext, CountDownLatch readerLatch) {
    super(metricsMaster, masterContext);
    mLatch = readerLatch;
  }

  public SignalBlockMaster(MetricsMaster mMetricsMaster, CoreMasterContext masterContext, ManualClock mClock, ExecutorServiceFactory constantExecutorServiceFactory, CountDownLatch targetLatch) {
    super(mMetricsMaster, masterContext, mClock, constantExecutorServiceFactory);
    mLatch = targetLatch;
  }

  public void setLatch(CountDownLatch newLatch) {
    mLatch = newLatch;
  }

  @Override
  LockResource lockBlock(long blockId) {
    LockResource res = super.lockBlock(blockId);
    // The latch can receive more signals than the countdown number
    // But the CountdownLatch guarantees nothing happens if the countdown is already 0
    mLatch.countDown();
    return res;
  }
}
