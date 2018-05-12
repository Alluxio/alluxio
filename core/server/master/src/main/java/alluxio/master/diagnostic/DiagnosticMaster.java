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

package alluxio.master.diagnostic;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.Server;
import alluxio.clock.SystemClock;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.AbstractNonJournaledMaster;
import alluxio.master.MasterContext;
import alluxio.master.MasterProcess;
import alluxio.master.MasterRegistry;
import alluxio.master.block.BlockMaster;
import alluxio.master.callhome.CallHomeInfo;
import alluxio.master.callhome.CallHomeUtils;
import alluxio.master.file.FileSystemMaster;
import alluxio.util.executor.ExecutorServiceFactories;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This service periodically collects diagnostic information and stores it to a log.
 */
@ThreadSafe
public final class DiagnosticMaster extends AbstractNonJournaledMaster {
  private static final Logger LOG = LoggerFactory.getLogger(DiagnosticMaster.class);
  private static final Set<Class<? extends Server>> DEPS =
      ImmutableSet.of(BlockMaster.class, FileSystemMaster.class);

  /** The Alluxio master process. */
  private MasterProcess mMasterProcess;
  private long mIntervalMs;

  /**
   * The service that logs diagnostic info.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mDiagnosticService;

  /**
   * Creates a new instance of {@link DiagnosticMaster}.
   *
   * @param registry the master registry
   * @param masterContext the context for Alluxio master
   */
  public DiagnosticMaster(MasterRegistry registry, MasterContext masterContext) {
    super(masterContext, new SystemClock(), ExecutorServiceFactories
        .fixedThreadPoolExecutorServiceFactory(Constants.DIAGNOSTIC_MASTER_NAME, 2));
    registry.add(DiagnosticMaster.class, this);
    mIntervalMs = Configuration.getMs(PropertyKey.DIAGNOSTIC_LOG_INTERVAL_MS);
  }

  /**
   * Sets the master to be used in {@link DiagnosticExecutor} for collecting diagnostic information.
   * This should be called before calling {@link #start(Boolean)}.
   *
   * @param masterProcess the Alluxio master process
   */
  public void setMaster(MasterProcess masterProcess) {
    mMasterProcess = masterProcess;
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return DEPS;
  }

  @Override
  public String getName() {
    return Constants.DIAGNOSTIC_MASTER_NAME;
  }

  @Override
  public void start(Boolean isLeader) throws IOException {
    super.start(isLeader);
    Preconditions.checkNotNull(mMasterProcess, "Alluxio master process is not specified");
    LOG.info("Starting {}", getName());
    mDiagnosticService = getExecutorService().submit(
        new HeartbeatThread(HeartbeatContext.MASTER_DIAGNOSTIC, new DiagnosticExecutor(mMasterProcess),
            mIntervalMs));
    LOG.info("{} is started", getName());
  }

  @Override
  public Map<String, TProcessor> getServices() {
    return new HashMap<>();
  }

  /**
   * Collects and saves diagnostic information during the heartbeat.
   */
  @ThreadSafe
  public static final class DiagnosticExecutor implements HeartbeatExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(DiagnosticExecutor.class);
    private static final Logger DIAGNOSTIC_LOG =
        LoggerFactory.getLogger("DIAGNOSTIC_LOG");

    private MasterProcess mMasterProcess;
    private BlockMaster mBlockMaster;
    private FileSystemMaster mFsMaster;

    /**
     * Creates a new instance of {@link DiagnosticExecutor}.
     *
     * @param masterProcess the Alluxio master process
     */
    public DiagnosticExecutor(MasterProcess masterProcess) {
      mMasterProcess = masterProcess;
      mBlockMaster = masterProcess.getMaster(BlockMaster.class);
      mFsMaster = masterProcess.getMaster(FileSystemMaster.class);
    }

    @Override
    public void heartbeat() throws InterruptedException {
      try {
        if (!mMasterProcess.isServing()) {
          DIAGNOSTIC_LOG.info("Master is not serving yet.");
          return;
        }
        CallHomeInfo info = CallHomeUtils.collectDiagnostics(mMasterProcess, mBlockMaster,
            mFsMaster);
        if (info == null) {
          DIAGNOSTIC_LOG.info("Diagnostic information not ready.");
          return;
        }
        try {
          logDiagnosticInfo(info);
        } catch (IOException e) {
          LOG.warn("Failed to log diagnostic info: {}", e.getMessage());
        }
      } catch (IOException e) {
        LOG.warn("Failed to collect diagnostic info: {}", e.getMessage());
      }
    }

    @Override
    public void close() {
      // Nothing to close.
    }

    /**
     * Log diagnostic info to configured log file.
     *
     * @param info diagnostic info
     * @throws IOException when failed to write to log file
     */
    private void logDiagnosticInfo(CallHomeInfo info) throws IOException {
      // Encode info into json as payload.
      String payload;
      try {
        ObjectMapper mapper = new ObjectMapper();
        payload = mapper.writeValueAsString(info);
      } catch (JsonProcessingException e) {
        throw new IOException("Failed to encode CallHomeInfo as json: " + e);
      }
      DIAGNOSTIC_LOG.info(payload);
    }
  }
}
