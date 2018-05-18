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
import alluxio.master.SafeModeManager;
import alluxio.master.block.BlockMaster;
import alluxio.master.meta.checkconf.ServerConfigurationChecker;
import alluxio.master.meta.checkconf.ServerConfigurationStore;
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
import alluxio.wire.Address;
import alluxio.wire.ConfigProperty;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The default meta master.
 */
@NotThreadSafe
public final class DefaultMetaMaster extends AbstractMaster implements MetaMaster {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetaMaster.class);
  private static final Set<Class<? extends Server>> DEPS =
      ImmutableSet.<Class<? extends Server>>of(BlockMaster.class);

  // Master metadata management.
  private static final IndexDefinition<MasterInfo> ID_INDEX =
      new IndexDefinition<MasterInfo>(true) {
        @Override
        public Object getFieldValue(MasterInfo o) {
          return o.getId();
        }
      };

  private static final IndexDefinition<MasterInfo> ADDRESS_INDEX =
      new IndexDefinition<MasterInfo>(true) {
        @Override
        public Object getFieldValue(MasterInfo o) {
          return o.getAddress();
        }
      };

  /** Handle to the block master. */
  private final BlockMaster mBlockMaster;

  /** The clock to use for determining the time. */
  private final Clock mClock = new SystemClock();

  /** The master configuration store. */
  private final ServerConfigurationStore mMasterConfigStore = new ServerConfigurationStore();
  /** The worker configuration store. */
  private final ServerConfigurationStore mWorkerConfigStore = new ServerConfigurationStore();
  /** The server-side configuration checker. */
  private final ServerConfigurationChecker mConfigChecker =
      new ServerConfigurationChecker(mMasterConfigStore, mWorkerConfigStore);

  /** Keeps track of standby masters which are in communication with the leader master. */
  private final IndexedSet<MasterInfo> mMasters =
      new IndexedSet<>(ID_INDEX, ADDRESS_INDEX);
  /** Keeps track of standby masters which are no longer in communication with the leader master. */
  private final IndexedSet<MasterInfo> mLostMasters =
      new IndexedSet<>(ID_INDEX, ADDRESS_INDEX);

  /** The address of this master. */
  private final Address mMasterAddress = new Address()
      .setHost(Configuration.get(PropertyKey.MASTER_HOSTNAME))
      .setRpcPort(Configuration.getInt(PropertyKey.MASTER_RPC_PORT));

  /** The connect address for the rpc server. */
  private final InetSocketAddress mRpcConnectAddress
      = NetworkAddressUtils.getConnectAddress(NetworkAddressUtils.ServiceType.MASTER_RPC);

  /** The manager of safe mode state. */
  private final SafeModeManager mSafeModeManager;

  /** The start time for when the master started serving the RPC server. */
  private final long mStartTimeMs;

  /** The master ID for this master. */
  private AtomicReference<Long> mMasterId = new AtomicReference<>(-1L);

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
    mSafeModeManager = masterContext.getmSafeModeManager();
    mStartTimeMs = masterContext.getStartTimeMs();
    mBlockMaster = blockMaster;
    mBlockMaster.registerLostWorkerFoundListener(this::lostWorkerFoundHandler);
    mBlockMaster.registerWorkerLostListener(this::workerLostHandler);
    mBlockMaster.registerNewWorkerConfListener(this::registerNewWorkerConfHandler);
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
  public void processJournalEntry(JournalEntry entry) throws IOException {
    throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(entry));
  }

  @Override
  public void resetState() {}

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    return Iterators.emptyIterator();
  }

  @Override
  public void start(Boolean isPrimary) throws IOException {
    super.start(isPrimary);
    mWorkerConfigStore.reset();
    mMasterConfigStore.reset();
    if (isPrimary) {
      // Add the configuration of the current leader master
      mMasterConfigStore.registerNewConf(mMasterAddress,
          Configuration.getConfiguration(PropertyKey.Scope.MASTER));

      // The service that detects lost standby master nodes
      getExecutorService().submit(new HeartbeatThread(
          HeartbeatContext.MASTER_LOST_MASTER_DETECTION,
          new LostMasterDetectionHeartbeatExecutor(),
          (int) Configuration.getMs(PropertyKey.MASTER_HEARTBEAT_INTERVAL_MS)));
    } else {
      // Standby master should setup MetaMasterSync to communicate with the leader master
      MetaMasterMasterClient metaMasterClient =
          new MetaMasterMasterClient(MasterClientConfig.defaults());
      setMasterId(metaMasterClient);
      MetaMasterSync metaMasterSync =
          new MetaMasterSync(mMasterId, mMasterAddress, metaMasterClient);
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
  public ServerConfigurationChecker.ConfigCheckReport getConfigCheckReport() {
    return mConfigChecker.getConfigCheckReport();
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
  public long getMasterId(Address address) {
    MasterInfo existingMaster = mMasters.getFirstByField(ADDRESS_INDEX, address);
    if (existingMaster != null) {
      // This master address is already mapped to a master id.
      long oldMasterId = existingMaster.getId();
      LOG.warn("The master {} already exists as id {}.", address, oldMasterId);
      return oldMasterId;
    }

    MasterInfo lostMaster = mLostMasters.getFirstByField(ADDRESS_INDEX, address);
    if (lostMaster != null) {
      // This is one of the lost masters
      mMasterConfigStore.lostNodeFound(lostMaster.getAddress());
      synchronized (lostMaster) {
        final long lostMasterId = lostMaster.getId();
        LOG.warn("A lost master {} has requested its old id {}.", address, lostMasterId);

        // Update the timestamp of the master before it is considered an active master.
        lostMaster.updateLastUpdatedTimeMs();
        mMasters.add(lostMaster);
        mLostMasters.remove(lostMaster);
        return lostMasterId;
      }
    }

    // Generate a new master id.
    long masterId = IdUtils.getRandomNonNegativeLong();
    while (!mMasters.add(new MasterInfo(masterId, address))) {
      masterId = IdUtils.getRandomNonNegativeLong();
    }

    LOG.info("getMasterId(): MasterAddress: {} id: {}", address, masterId);
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
    return mSafeModeManager.isInSafeMode();
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
    mMasterConfigStore.registerNewConf(master.getAddress(), configList);

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
                master.getAddress(), lastUpdate);
            mLostMasters.add(master);
            mMasters.remove(master);
            mMasterConfigStore.handleNodeLost(master.getAddress());
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
   * Updates the config checker when a lost worker becomes alive.
   *
   * @param address the address of the worker
   */
  private void lostWorkerFoundHandler(Address address) {
    mWorkerConfigStore.lostNodeFound(address);
  }

  /**
   * Updates the config checker when a live worker becomes lost.
   *
   * @param address the address of the worker
   */
  private void workerLostHandler(Address address) {
    mWorkerConfigStore.handleNodeLost(address);
  }

  /**
   * Updates the config checker when a worker registers with configuration.
   *
   * @param address the address of the worker
   * @param configList the configuration of this worker
   */
  private void registerNewWorkerConfHandler(Address address, List<ConfigProperty> configList) {
    mWorkerConfigStore.registerNewConf(address, configList);
  }

  /**
   * Sets the master id. This method should only be called when this master is a standby master.
   *
   * @param metaMasterClient the meta master client to communicate with leader master
   */
  private void setMasterId(MetaMasterMasterClient metaMasterClient) {
    try {
      RetryUtils.retry("get master id",
          () -> mMasterId.set(metaMasterClient.getId(mMasterAddress)),
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
