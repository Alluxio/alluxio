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

package alluxio.master.throttle;

import alluxio.Constants;
import alluxio.Server;
import alluxio.annotation.SuppressFBWarnings;
import alluxio.clock.SystemClock;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.AbstractMaster;
import alluxio.master.MasterContext;
import alluxio.master.MasterProcess;
import alluxio.master.MasterRegistry;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.NoopJournaled;
import alluxio.master.metrics.MetricsMaster;
import alluxio.util.executor.ExecutorServiceFactories;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This service periodically monitors the system and throttles in case of busy.
 */
@ThreadSafe
public final class DefaultThrottleMaster extends AbstractMaster implements NoopJournaled {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultThrottleMaster.class);
  private static final Set<Class<? extends Server>> DEPS =
      ImmutableSet.of(BlockMaster.class, FileSystemMaster.class,
          MetricsMaster.class);

  /** The Alluxio master process. */
  private MasterProcess mMasterProcess;

  private ThrottleExecutor mThrottleExecutor;

  /**
   * The service that performs license check.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mThrottleService;

  /**
   * Creates a new instance of {@link DefaultThrottleMaster}.
   *
   * @param registry the master registry
   * @param masterContext the context for Alluxio master
   */
  public DefaultThrottleMaster(MasterRegistry registry, MasterContext masterContext) {
    super(masterContext, new SystemClock(), ExecutorServiceFactories
        .cachedThreadPool(Constants.THROTTLE_MASTER_NAME));
    registry.add(DefaultThrottleMaster.class, this);
  }

  /**
   * Sets the master to be used in {@link ThrottleExecutor} for throttling.
   * This should be called before calling {@link #start(Boolean)}.
   *
   * @param masterProcess the Alluxio master process
   */
  public void setMaster(MasterProcess masterProcess) {
    mMasterProcess = masterProcess;
    mThrottleExecutor = new ThrottleExecutor(mMasterProcess);
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return DEPS;
  }

  @Override
  public String getName() {
    return Constants.THROTTLE_MASTER_NAME;
  }

  @Override
  public void start(Boolean isLeader) throws IOException {
    super.start(isLeader);
    Preconditions.checkNotNull(mMasterProcess, "Alluxio master process is not specified");
    Preconditions.checkNotNull(mThrottleExecutor, "ThrottleExecutor is not specified");
    if (!isLeader) {
      return;
    }
    LOG.info("Starting {}", getName());
    mThrottleService = getExecutorService().submit(
        new HeartbeatThread(HeartbeatContext.MASTER_THROTTLE, mThrottleExecutor,
            () -> Configuration.getMs(PropertyKey.MASTER_THROTTLE_HEARTBEAT_INTERVAL),
            Configuration.global(),
            mMasterContext.getUserState()));
    LOG.info("{} is started", getName());
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    return new HashMap<>();
  }

  /**
   * Collects and saves call home information during the heartbeat.
   */
  @ThreadSafe
  public static final class ThrottleExecutor implements HeartbeatExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(ThrottleExecutor.class);

    private MasterProcess mMasterProcess;
    private SystemMonitor mSystemMonitor;

    /**
     * Creates a new instance of {@link ThrottleExecutor}.
     *
     * @param masterProcess the Alluxio master process
     */
    public ThrottleExecutor(MasterProcess masterProcess) {
      mMasterProcess = masterProcess;
      mSystemMonitor = new SystemMonitor(mMasterProcess);
    }

    @Override
    public void heartbeat() throws InterruptedException {
      mSystemMonitor.run();
    }

    @Override
    public void close() {
      // Nothing to close.
    }
  }
}

