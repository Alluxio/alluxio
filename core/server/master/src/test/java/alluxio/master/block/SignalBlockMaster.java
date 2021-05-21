package alluxio.master.block;

import alluxio.Server;
import alluxio.StorageTierAssoc;
import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.clock.ManualClock;
import alluxio.exception.BlockInfoException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Command;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GrpcService;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.ServiceType;
import alluxio.grpc.StorageList;
import alluxio.grpc.WorkerLostStorageInfo;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterContext;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.metrics.MetricsMaster;
import alluxio.metrics.Metric;
import alluxio.proto.journal.Journal;
import alluxio.proto.meta.Block;
import alluxio.resource.CloseableIterator;
import alluxio.resource.LockResource;
import alluxio.util.CommonUtils;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.wire.Address;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

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
