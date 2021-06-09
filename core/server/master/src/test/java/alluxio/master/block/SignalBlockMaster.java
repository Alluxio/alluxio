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

package alluxio.master.block;

import alluxio.clock.ManualClock;
import alluxio.master.CoreMasterContext;
import alluxio.master.metrics.MetricsMaster;
import alluxio.resource.LockResource;
import alluxio.util.executor.ExecutorServiceFactory;

import java.util.concurrent.CountDownLatch;

/**
 * When the writer is writing, issue a signal so other readers/writers know when to start.
 * A {@link CountDownLatch} will be used to pass the signal.
 * Other readers/writers will wait on the signal to execute.
 */
class SignalBlockMaster extends DefaultBlockMaster {
  CountDownLatch mLatch;

  SignalBlockMaster(MetricsMaster metricsMaster,
                    CoreMasterContext masterContext,
                    CountDownLatch readerLatch) {
    super(metricsMaster, masterContext);
    mLatch = readerLatch;
  }

  SignalBlockMaster(MetricsMaster mMetricsMaster,
                    CoreMasterContext masterContext,
                    ManualClock mClock,
                    ExecutorServiceFactory constantExecutorServiceFactory,
                    CountDownLatch targetLatch) {
    super(mMetricsMaster, masterContext, mClock, constantExecutorServiceFactory);
    mLatch = targetLatch;
  }

  void setLatch(CountDownLatch newLatch) {
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
