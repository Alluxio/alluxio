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

package alluxio.master.meta;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.Server;
import alluxio.clock.SystemClock;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.NotFoundException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.AbstractMaster;
import alluxio.master.MasterClientConfig;
import alluxio.master.MasterContext;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectory;
import alluxio.proto.journal.Journal;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.retry.ExponentialTimeBoundedRetry;
import alluxio.retry.RetryUtils;
import alluxio.thrift.MetaCommand;
import alluxio.thrift.MetaMasterClientService;
import alluxio.thrift.MetaMasterMasterService;
import alluxio.thrift.RegisterMasterTOptions;
import alluxio.util.IdUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.ConfigProperty;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The master that handles all metadata management.
 */
@NotThreadSafe
public final class DefaultMetaMaster extends AbstractMaster implements MetaMaster {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetaMaster.class);
  private static final Set<Class<? extends Server>> DEPS =
      ImmutableSet.<Class<? extends Server>>of(BlockMaster.class);

  /** Indexes to help find master information in indexed set. */
  private static final IndexDefinition<MasterInfo> ID_INDEX =
      new IndexDefinition<MasterInfo>(true) {
        @Override
        public Object getFieldValue(MasterInfo o) {
          return o.getId();
        }
      };
  private static final IndexDefinition<MasterInfo> HOSTNAME_INDEX =
      new IndexDefinition<MasterInfo>(true) {
        @Override
        public Object getFieldValue(MasterInfo o) {
          return o.getHostname();
        }
      };

  /** Handle to the block master. */
  private final BlockMaster mBlockMaster;

  /** The clock to use for determining the time. */
  private final Clock mClock = new SystemClock();

  /** The connect address for the rpc server. */
  private final InetSocketAddress mRpcConnectAddress
      = NetworkAddressUtils.getConnectAddress(NetworkAddressUtils.ServiceType.MASTER_RPC);

  /** is true if the master is in safe mode. */
  private boolean mIsInSafeMode;

  /** Keeps track of standby masters which are in communication with the leader master. */
  private final IndexedSet<MasterInfo> mMasters =
      new IndexedSet<>(ID_INDEX, HOSTNAME_INDEX);
  /** Keeps track of standby masters which are no longer in communication with the leader master. */
  private final IndexedSet<MasterInfo> mLostMasters =
      new IndexedSet<>(ID_INDEX, HOSTNAME_INDEX);

  /** The master configuration checker. */
  private final ServerConfigurationChecker mMasterConfigChecker = new ServerConfigurationChecker();
  /** The worker configuration checker. */
  private final ServerConfigurationChecker mWorkerConfigChecker = new ServerConfigurationChecker();

  /** The hostname of this master. */
  private String mMasterHostname = Configuration.get(PropertyKey.MASTER_HOSTNAME);

  /** The master ID for this master. */
  private AtomicReference<Long> mMasterId = new AtomicReference<>(-1L);

  /** Client for all meta master communication. */
  private final MetaMasterMasterClient mMetaMasterClient
      = new MetaMasterMasterClient(MasterClientConfig.defaults());

  /** The start time for when the master started serving the RPC server. */
  private long mStartTimeMs = -1;

  /**
   * Creates a new instance of {@link DefaultMetaMaster}.
   *
   * @param blockMaster a block master handle
   * @param masterContext the context for Alluxio master
   */
  DefaultMetaMaster(BlockMaster blockMaster, MasterContext masterContext) {
    this(blockMaster, masterContext, ExecutorServiceFactories
        .fixedThreadPoolExecutorServiceFactory(Constants.META_MASTER_NAME, 2));
  }

  /**
   * Creates a new instance of {@link DefaultMetaMaster}.
   *
   * @param blockMaster a block master handle
   * @param masterContext the context for Alluxio master
   * @param executorServiceFactory a factory for creating the executor service to use for running
   *        maintenance threads
   */
  DefaultMetaMaster(BlockMaster blockMaster, MasterContext masterContext,
      ExecutorServiceFactory executorServiceFactory) {
    super(masterContext, new SystemClock(), executorServiceFactory);
    mBlockMaster = blockMaster;

    mIsInSafeMode = masterContext.getmSafeModeManager().isInSafeMode();
    mBlockMaster.registerLostWorkerFoundListener(this::lostWorkerFoundHandler);
    mBlockMaster.registerWorkerLostListener(this::workerLostHandler);
    mBlockMaster.registerNewWorkerConfListener(this::registerNewWorkerConfHandler);

    Metrics.registerGauges(this);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<>();
    services.put(Constants.META_MASTER_CLIENT_SERVICE_NAME,
        new MetaMasterClientService.Processor<>(new MetaMasterClientServiceHandler(this)));
    services.put(Constants.META_MASTER_MASTER_SERVICE_NAME,
        new MetaMasterMasterService.Processor<>(new MetaMasterMasterServiceHandler(this)));
    return services;
  }

  @Override
  public String getName() {
    return Constants.META_MASTER_NAME;
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return DEPS;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) {}

  @Override
  public void resetState() {}

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    final Queue<Inode<?>> inodes = new LinkedList<>();
    return new Iterator<Journal.JournalEntry>() {
      @Override
      public boolean hasNext() {
        return !inodes.isEmpty();
      }

      @Override
      public Journal.JournalEntry next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        Inode<?> inode = inodes.poll();
        if (inode.isDirectory()) {
          inodes.addAll(((InodeDirectory) inode).getChildren());
        }
        return inode.toJournalEntry();
      }
    };
  }

  @Override
  public void start(Boolean isPrimary) throws IOException {
    super.start(isPrimary);
    mWorkerConfigChecker.reset();
    mStartTimeMs = System.currentTimeMillis();
    if (isPrimary) {
      mSafeModeManager.notifyPrimaryMasterStarted();

      //  The service that detects lost standby master nodes
      getExecutorService().submit(new HeartbeatThread(
          HeartbeatContext.MASTER_LOST_MASTER_DETECTION,
          new LostMasterDetectionHeartbeatExecutor(),
          (int) Configuration.getMs(PropertyKey.MASTER_HEARTBEAT_INTERVAL_MS)));
    } else {
      // Standby master should setup MetaMasterSync to communicate with the leader master
      setMasterId();
      MetaMasterSync metaMasterSync =
          new MetaMasterSync(mMasterId, mMasterHostname, mMetaMasterClient);
      getExecutorService().submit(new HeartbeatThread(HeartbeatContext.META_MASTER_SYNC,
          metaMasterSync, (int) Configuration.getMs(PropertyKey.MASTER_HEARTBEAT_INTERVAL_MS)));
      LOG.info("Standby master with id {} starts sending heartbeat to leader master.", mMasterId);
    }
  }

  @Override
  public void stop() throws IOException {
    super.stop();
  }

  @Override
  public List<ConfigProperty> getConfiguration() {
    List<ConfigProperty> configInfoList = new ArrayList<>();
    String alluxioConfPrefix = "alluxio";
    for (Map.Entry<String, String> entry : Configuration.toMap().entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(alluxioConfPrefix)) {
        PropertyKey propertyKey = PropertyKey.fromString(key);
        String source = Configuration.getFormattedSource(propertyKey);
        configInfoList.add(new ConfigProperty()
            .setName(key).setValue(entry.getValue()).setSource(source));
      }
    }
    return configInfoList;
  }

  @Override
  public long getMasterId(String hostname) {
    MasterInfo existingMaster = mMasters.getFirstByField(HOSTNAME_INDEX, hostname);
    if (existingMaster != null) {
      // This master hostname is already mapped to a master id.
      long oldMasterId = existingMaster.getId();
      LOG.warn("The master {} already exists as id {}.", hostname, oldMasterId);
      return oldMasterId;
    }

    MasterInfo lostMaster = mLostMasters.getFirstByField(HOSTNAME_INDEX, hostname);
    if (lostMaster != null) {
      // This is one of the lost masters
      mMasterConfigChecker.lostNodeFound(lostMaster.getId());
      synchronized (lostMaster) {
        final long lostMasterId = lostMaster.getId();
        LOG.warn("A lost master {} has requested its old id {}.", hostname, lostMasterId);

        // Update the timestamp of the master before it is considered an active master.
        lostMaster.updateLastUpdatedTimeMs();
        mMasters.add(lostMaster);
        mLostMasters.remove(lostMaster);
        return lostMasterId;
      }
    }

    // Generate a new master id.
    long masterId = IdUtils.getRandomNonNegativeLong();
    while (!mMasters.add(new MasterInfo(masterId, hostname))) {
      masterId = IdUtils.getRandomNonNegativeLong();
    }

    LOG.info("getMasterId(): Hostname: {} id: {}", hostname, masterId);
    return masterId;
  }

  @Override
  public InetSocketAddress getRpcAddress() {
    return mRpcConnectAddress;
  }

  @Override
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  @Override
  public long getUptimeMs() {
    return System.currentTimeMillis() - mStartTimeMs;
  }

  @Override
  public int getWebPort() {
    return Configuration.getInt(PropertyKey.MASTER_WEB_PORT);
  }

  @Override
  public boolean isInSafeMode() {
    return mIsInSafeMode;
  }

  @Override
  public MetaCommand masterHeartbeat(long masterId) {
    MasterInfo master = mMasters.getFirstByField(ID_INDEX, masterId);
    if (master == null) {
      LOG.warn("Could not find master id: {} for heartbeat.", masterId);
      return MetaCommand.Register;
    }

    master.updateLastUpdatedTimeMs();
    return MetaCommand.Nothing;
  }

  @Override
  public void masterRegister(long masterId, RegisterMasterTOptions options)
      throws NotFoundException {
    MasterInfo master = mMasters.getFirstByField(ID_INDEX, masterId);
    if (master == null) {
      throw new NotFoundException(ExceptionMessage.NO_MASTER_FOUND.getMessage(masterId));
    }

    master.updateLastUpdatedTimeMs();

    List<ConfigProperty> configList = options.getConfigList().stream()
        .map(ConfigProperty::fromThrift).collect(Collectors.toList());
    mMasterConfigChecker.registerNewConf(masterId, configList);

    LOG.info("registerMaster(): master: {} options: {}", master, options);
  }

  /**
   * Lost master periodic check.
   */
  private final class LostMasterDetectionHeartbeatExecutor implements HeartbeatExecutor {

    /**
     * Constructs a new {@link LostMasterDetectionHeartbeatExecutor}.
     */
    public LostMasterDetectionHeartbeatExecutor() {
    }

    @Override
    public void heartbeat() {
      int masterTimeoutMs = (int) Configuration.getMs(PropertyKey.MASTER_HEARTBEAT_TIMEOUT_MS);
      for (MasterInfo master : mMasters) {
        synchronized (master) {
          final long lastUpdate = mClock.millis() - master.getLastUpdatedTimeMs();
          if (lastUpdate > masterTimeoutMs) {
            LOG.error("The master {}({}) timed out after {}ms without a heartbeat!", master.getId(),
                master.getHostname(), lastUpdate);
            mLostMasters.add(master);
            mMasters.remove(master);
            mMasterConfigChecker.handleNodeLost(master.getId());
          }
        }
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }

  /**
   * Class that contains metrics related to MetaMaster.
   */
  public static final class Metrics {

    /**
     * Registers metric gauges.
     *
     * @param master the meta master handle
     */
    public static void registerGauges(final MetaMaster master) {}

    private Metrics() {} // prevent instantiation
  }

  /**
   * Updates the config checker when a lost worker becomes alive.
   *
   * @param id the id of the worker
   */
  private void lostWorkerFoundHandler(long id) {
    mWorkerConfigChecker.lostNodeFound(id);
  }

  /**
   * Updates the config checker when a live worker becomes lost.
   *
   * @param id the id of the worker
   */
  private void workerLostHandler(long id) {
    mWorkerConfigChecker.handleNodeLost(id);
  }

  /**
   * Updates the config checker when a worker registers with configuration.
   *
   * @param id the id of the worker
   * @param configList the configuration of this worker
   */
  private void registerNewWorkerConfHandler(long id, List<ConfigProperty> configList) {
    mWorkerConfigChecker.registerNewConf(id, configList);
  }

  /**
   * Sets the master id. This method should only be called when this master is a standby master.
   */
  private void setMasterId() {
    try {
      RetryUtils.retry("get master id",
          () -> mMasterId.set(mMetaMasterClient.getId(mMasterHostname)),
          ExponentialTimeBoundedRetry.builder()
              .withMaxDuration(Duration
                  .ofMillis(Configuration.getMs(PropertyKey.USER_RPC_RETRY_MAX_NUM_RETRY)))
              .withInitialSleep(Duration.ofMillis(100))
              .withMaxSleep(Duration.ofSeconds(5))
              .build());
    } catch (Exception e) {
      throw new RuntimeException("Failed to get a master id from leader master: " + e.getMessage());
    }

    Preconditions.checkNotNull(mMasterId, "mMasterId");
  }
}
