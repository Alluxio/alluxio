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

import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.Server;
import alluxio.clock.SystemClock;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.conf.Configuration;
import alluxio.conf.ConfigurationValueOptions;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.BackupPOptions;
import alluxio.grpc.BackupPRequest;
import alluxio.grpc.BackupStatusPRequest;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.GrpcService;
import alluxio.grpc.MetaCommand;
import alluxio.grpc.RegisterMasterPOptions;
import alluxio.grpc.Scope;
import alluxio.grpc.ServiceType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.CoreMaster;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterClientContext;
import alluxio.master.StateLockOptions;
import alluxio.master.backup.BackupLeaderRole;
import alluxio.master.backup.BackupRole;
import alluxio.master.backup.BackupWorkerRole;
import alluxio.master.block.BlockMaster;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalType;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.meta.checkconf.ConfigurationChecker;
import alluxio.master.meta.checkconf.ConfigurationStore;
import alluxio.proto.journal.Journal;
import alluxio.proto.journal.Meta;
import alluxio.resource.CloseableIterator;
import alluxio.underfs.UfsManager;
import alluxio.util.ConfigurationUtils;
import alluxio.util.IdUtils;
import alluxio.util.OSUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.Address;
import alluxio.wire.BackupStatus;
import alluxio.wire.ConfigCheckReport;
import alluxio.wire.ConfigHash;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.MessageFormat;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The default meta master.
 */
@NotThreadSafe
public final class DefaultMetaMaster extends CoreMaster implements MetaMaster {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetaMaster.class);
  private static final Set<Class<? extends Server>> DEPS = ImmutableSet.of(BlockMaster.class);

  // Master metadata management.
  private static final IndexDefinition<MasterInfo, Long> ID_INDEX =
      IndexDefinition.ofUnique(MasterInfo::getId);

  private static final IndexDefinition<MasterInfo, Address> ADDRESS_INDEX =
      IndexDefinition.ofUnique(MasterInfo::getAddress);

  /** Core master context. */
  private final CoreMasterContext mCoreMasterContext;

  /** The clock to use for determining the time. */
  private final Clock mClock = new SystemClock();

  /** The master configuration store. */
  private final ConfigurationStore mMasterConfigStore = new ConfigurationStore();
  /** The worker configuration store. */
  private final ConfigurationStore mWorkerConfigStore = new ConfigurationStore();
  /** The server-side configuration checker. */
  private final ConfigurationChecker mConfigChecker =
      new ConfigurationChecker(mMasterConfigStore, mWorkerConfigStore);

  /** Keeps track of standby masters which are in communication with the leader master. */
  private final IndexedSet<MasterInfo> mMasters =
      new IndexedSet<>(ID_INDEX, ADDRESS_INDEX);
  /** Keeps track of standby masters which are no longer in communication with the leader master. */
  private final IndexedSet<MasterInfo> mLostMasters =
      new IndexedSet<>(ID_INDEX, ADDRESS_INDEX);

  /** The connect address for the rpc server. */
  private final InetSocketAddress mRpcConnectAddress
      = NetworkAddressUtils.getConnectAddress(NetworkAddressUtils.ServiceType.MASTER_RPC,
      Configuration.global());

  /** Indicates if newer version is available. */
  private boolean mNewerVersionAvailable;

  /** The address of this master. */
  private final Address mMasterAddress;

  /** The manager of all ufs. */
  private final UfsManager mUfsManager;

  /** The metadata daily backup. */
  private DailyMetadataBackup mDailyBackup;

  /** Path level properties. */
  private final PathProperties mPathProperties;

  /** Persisted state for MetaMaster. */
  private final State mState;

  /** Value to be used for the cluster ID when not assigned. */
  public static final String INVALID_CLUSTER_ID = "INVALID_CLUSTER_ID";

  /** Used to manage backup role. */
  private BackupRole mBackupRole;

  @Nullable
  private final JournalSpaceMonitor mJournalSpaceMonitor;

  /**
   * Journaled state for MetaMaster.
   */
  @NotThreadSafe
  public static final class State implements alluxio.master.journal.Journaled {
    /** A unique ID to identify the cluster. */
    private String mClusterID = INVALID_CLUSTER_ID;

    /**
     * @return the cluster ID
     */
    public String getClusterID() {
      return mClusterID;
    }

    @Override
    public CheckpointName getCheckpointName() {
      return CheckpointName.CLUSTER_INFO;
    }

    @Override
    public boolean processJournalEntry(Journal.JournalEntry entry) {
      if (entry.hasClusterInfo()) {
        mClusterID = entry.getClusterInfo().getClusterId();
        return true;
      }
      return false;
    }

    /**
     * @param ctx the journal context
     * @param clusterId the clusterId journal clusterId
     */
    public void applyAndJournal(java.util.function.Supplier<JournalContext> ctx, String clusterId) {
      applyAndJournal(ctx,
          Journal.JournalEntry.newBuilder()
              .setClusterInfo(Meta.ClusterInfoEntry.newBuilder().setClusterId(clusterId).build())
              .build());
    }

    @Override
    public void resetState() {
      mClusterID = INVALID_CLUSTER_ID;
    }

    @Override
    public CloseableIterator<Journal.JournalEntry> getJournalEntryIterator() {
      if (mClusterID.equals(INVALID_CLUSTER_ID)) {
        return CloseableIterator.noopCloseable(Collections.emptyIterator());
      }
      return CloseableIterator.noopCloseable(Collections.singleton(Journal.JournalEntry.newBuilder()
          .setClusterInfo(Meta.ClusterInfoEntry.newBuilder().setClusterId(mClusterID).build())
          .build()).iterator());
    }
  }

  /**
   * Creates a new instance of {@link DefaultMetaMaster}.
   *
   * @param blockMaster a block master handle
   * @param masterContext the context for Alluxio master
   */
  DefaultMetaMaster(BlockMaster blockMaster, CoreMasterContext masterContext) {
    this(blockMaster, masterContext,
        ExecutorServiceFactories.cachedThreadPool(Constants.META_MASTER_NAME));
  }

  /**
   * Creates a new instance of {@link DefaultMetaMaster}.
   *
   * @param blockMaster a block master handle
   * @param masterContext the context for Alluxio master
   * @param executorServiceFactory a factory for creating the executor service to use for running
   *        maintenance threads
   */
  DefaultMetaMaster(BlockMaster blockMaster, CoreMasterContext masterContext,
      ExecutorServiceFactory executorServiceFactory) {
    super(masterContext, new SystemClock(), executorServiceFactory);
    mCoreMasterContext = masterContext;
    mMasterAddress =
        new Address().setHost(Configuration.getOrDefault(PropertyKey.MASTER_HOSTNAME,
            "localhost"))
            .setRpcPort(mPort);
    /* Handle to the block master. */
    blockMaster.registerLostWorkerFoundListener(mWorkerConfigStore::lostNodeFound);
    blockMaster.registerWorkerLostListener(mWorkerConfigStore::handleNodeLost);
    blockMaster.registerNewWorkerConfListener(mWorkerConfigStore::registerNewConf);

    mUfsManager = masterContext.getUfsManager();

    mPathProperties = new PathProperties();
    mState = new State();
    if (Configuration.getEnum(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.class)
        .equals(JournalType.EMBEDDED) && OSUtils.isLinux()) {
      mJournalSpaceMonitor = new JournalSpaceMonitor(Configuration.global());
    } else {
      mJournalSpaceMonitor = null;
    }
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    Map<ServiceType, GrpcService> services = new HashMap<>();
    services.put(ServiceType.META_MASTER_CONFIG_SERVICE,
        new GrpcService(new MetaMasterConfigurationServiceHandler(this)).disableAuthentication());
    services.put(ServiceType.META_MASTER_CLIENT_SERVICE,
        new GrpcService(new MetaMasterClientServiceHandler(this)));
    services.put(ServiceType.META_MASTER_MASTER_SERVICE,
        new GrpcService(new MetaMasterMasterServiceHandler(this)));
    // Add backup role services.
    services.putAll(mBackupRole.getRoleServices());
    services.putAll(mJournalSystem.getJournalServices());
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
  public void start(Boolean isPrimary) throws IOException {
    super.start(isPrimary);
    mWorkerConfigStore.reset();
    mMasterConfigStore.reset();
    if (isPrimary) {
      // Add the configuration of the current leader master
      mMasterConfigStore.registerNewConf(mMasterAddress,
          Configuration.getConfiguration(Scope.MASTER));

      // The service that detects lost standby master nodes
      getExecutorService().submit(new HeartbeatThread(
          HeartbeatContext.MASTER_LOST_MASTER_DETECTION,
          new LostMasterDetectionHeartbeatExecutor(),
          (int) Configuration.getMs(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL),
          Configuration.global(), mMasterContext.getUserState()));
      getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_LOG_CONFIG_REPORT_SCHEDULING,
              new LogConfigReportHeartbeatExecutor(),
              (int) Configuration
                  .getMs(PropertyKey.MASTER_LOG_CONFIG_REPORT_HEARTBEAT_INTERVAL),
              Configuration.global(), mMasterContext.getUserState()));

      if (Configuration.getBoolean(PropertyKey.MASTER_DAILY_BACKUP_ENABLED)) {
        mDailyBackup = new DailyMetadataBackup(this, Executors.newSingleThreadScheduledExecutor(
            ThreadFactoryUtils.build("DailyMetadataBackup-%d", true)), mUfsManager);
        mDailyBackup.start();
      }
      if (mJournalSpaceMonitor != null) {
        getExecutorService().submit(new HeartbeatThread(
            HeartbeatContext.MASTER_JOURNAL_SPACE_MONITOR, mJournalSpaceMonitor,
            Configuration.getMs(PropertyKey.MASTER_JOURNAL_SPACE_MONITOR_INTERVAL),
            Configuration.global(), mMasterContext.getUserState()));
      }
      if (mState.getClusterID().equals(INVALID_CLUSTER_ID)) {
        try (JournalContext context = createJournalContext()) {
          String clusterID = java.util.UUID.randomUUID().toString();
          mState.applyAndJournal(context, clusterID);
          LOG.info("Created new cluster ID {}", clusterID);
        }
        if (Configuration.getBoolean(PropertyKey.MASTER_UPDATE_CHECK_ENABLED)
            && !Configuration.getBoolean(PropertyKey.TEST_MODE)) {
          getExecutorService().submit(new HeartbeatThread(HeartbeatContext.MASTER_UPDATE_CHECK,
              new UpdateChecker(this),
              (int) Configuration.getMs(PropertyKey.MASTER_UPDATE_CHECK_INTERVAL),
              Configuration.global(), mMasterContext.getUserState()));
        }
      } else {
        LOG.info("Detected existing cluster ID {}", mState.getClusterID());
      }
      mBackupRole = new BackupLeaderRole(mCoreMasterContext);
    } else {
      if (ConfigurationUtils.isHaMode(Configuration.global())) {
        // Standby master should setup MetaMasterSync to communicate with the leader master
        RetryHandlingMetaMasterMasterClient metaMasterClient =
            new RetryHandlingMetaMasterMasterClient(MasterClientContext
                .newBuilder(ClientContext.create(Configuration.global())).build());
        getExecutorService().submit(new HeartbeatThread(HeartbeatContext.META_MASTER_SYNC,
            new MetaMasterSync(mMasterAddress, metaMasterClient),
            (int) Configuration.getMs(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL),
            Configuration.global(), mMasterContext.getUserState()));
        LOG.info("Standby master with address {} starts sending heartbeat to leader master.",
            mMasterAddress);
      }
      // Enable worker role if backup delegation is enabled.
      if (Configuration.getBoolean(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED)) {
        mBackupRole = new BackupWorkerRole(mCoreMasterContext);
      }
    }
  }

  @Override
  public void stop() throws IOException {
    if (mDailyBackup != null) {
      mDailyBackup.stop();
      mDailyBackup = null;
    }
    if (mBackupRole != null) {
      mBackupRole.close();
      mBackupRole = null;
    }
    super.stop();
  }

  /**
   * Overrides current backup role and forces the master to take a local backup.
   * @return the {@link BackupStatus}
   * @throws AlluxioException if it encounters issues triggering the backup
   */
  public BackupStatus takeEmergencyBackup() throws AlluxioException {
    mBackupRole = new BackupLeaderRole(mCoreMasterContext);
    BackupPRequest request = BackupPRequest.newBuilder()
        .setOptions(BackupPOptions.newBuilder()
            .setAllowLeader(true)
            .setBypassDelegation(true)
            .setRunAsync(false)
            .build())
        .build();
    return backup(request, StateLockOptions.defaults());
  }

  @Override
  public BackupStatus backup(BackupPRequest request, StateLockOptions stateLockOptions)
      throws AlluxioException {
    return mBackupRole.backup(request, stateLockOptions);
  }

  @Override
  public BackupStatus getBackupStatus(BackupStatusPRequest statusPRequest) throws AlluxioException {
    return mBackupRole.getBackupStatus(statusPRequest);
  }

  @Override
  public String checkpoint() throws IOException {
    mJournalSystem.checkpoint(mMasterContext.getStateLockManager());
    return NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.MASTER_RPC,
        Configuration.global());
  }

  @Override
  public ConfigCheckReport getConfigCheckReport() {
    return mConfigChecker.getConfigCheckReport();
  }

  @Override
  public alluxio.wire.Configuration getConfiguration(GetConfigurationPOptions options) {
    // NOTE(cc): there is no guarantee that the returned cluster and path configurations are
    // consistent snapshot of the system's state at a certain time, the path configuration might
    // be in a newer state. But it's guaranteed that the hashes are respectively correspondent to
    // the properties.
    alluxio.wire.Configuration.Builder builder = alluxio.wire.Configuration.newBuilder();

    if (!options.getIgnoreClusterConf()) {
      for (PropertyKey key : Configuration.keySet()) {
        if (key.isBuiltIn()) {
          Source source = Configuration.getSource(key);
          Object value = Configuration.getOrDefault(key, null,
              ConfigurationValueOptions.defaults().useDisplayValue(true)
                  .useRawValue(options.getRawValue()));
          builder.addClusterProperty(key.getName(), value, source);
        }
      }
      // NOTE(cc): assumes that Configuration is read-only when master is running, otherwise,
      // the following hash might not correspond to the above cluster configuration.
      builder.setClusterConfHash(Configuration.hash());
    }

    if (!options.getIgnorePathConf()) {
      PathPropertiesView pathProperties = mPathProperties.snapshot();
      pathProperties.getProperties().forEach((path, properties) ->
          properties.forEach((key, value) ->
              builder.addPathProperty(path, key, value)));
      builder.setPathConfHash(pathProperties.getHash());
    }

    return builder.build();
  }

  @Override
  public ConfigHash getConfigHash() {
    return new ConfigHash(Configuration.hash(), mPathProperties.hash());
  }

  @Override
  public Optional<JournalSpaceMonitor> getJournalSpaceMonitor() {
    return Optional.ofNullable(mJournalSpaceMonitor);
  }

  @Override
  public void setPathConfiguration(String path, Map<PropertyKey, String> properties)
      throws UnavailableException {
    try (JournalContext ctx = createJournalContext()) {
      mPathProperties.add(ctx, path, properties);
    }
  }

  @Override
  public void removePathConfiguration(String path, Set<String> keys)
      throws UnavailableException {
    try (JournalContext ctx = createJournalContext()) {
      mPathProperties.remove(ctx, path, keys);
    }
  }

  @Override
  public void removePathConfiguration(String path) throws UnavailableException {
    try (JournalContext ctx = createJournalContext()) {
      mPathProperties.removeAll(ctx, path);
    }
  }

  @Override
  public void setNewerVersionAvailable(boolean available) {
    mNewerVersionAvailable = available;
  }

  @Override
  public boolean getNewerVersionAvailable() {
    return mNewerVersionAvailable;
  }

  @Override
  public List<Address> getMasterAddresses() {
    return mMasterConfigStore.getLiveNodeAddresses();
  }

  @Override
  public List<Address> getWorkerAddresses() {
    return mWorkerConfigStore.getLiveNodeAddresses();
  }

  @Override
  public alluxio.wire.MasterInfo[] getMasterInfos() {
    alluxio.wire.MasterInfo[] masterInfos = new alluxio.wire.MasterInfo[mMasters.size()];
    int indexNum = 0;
    for (MasterInfo master : mMasters) {
      masterInfos[indexNum] = new alluxio.wire.MasterInfo(master.getId(),
          master.getAddress(), master.getLastUpdatedTimeMs());
      indexNum++;
    }
    return masterInfos;
  }

  @Override
  public alluxio.wire.MasterInfo[] getLostMasterInfos() {
    alluxio.wire.MasterInfo[] masterInfos = new alluxio.wire.MasterInfo[mLostMasters.size()];
    int indexNum = 0;
    for (MasterInfo master : mLostMasters) {
      masterInfos[indexNum] = new alluxio.wire.MasterInfo(master.getId(),
          master.getAddress(), master.getLastUpdatedTimeMs());
      indexNum++;
    }
    return masterInfos;
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
      return MetaCommand.MetaCommand_Register;
    }

    master.updateLastUpdatedTimeMs();
    return MetaCommand.MetaCommand_Nothing;
  }

  @Override
  public void masterRegister(long masterId, RegisterMasterPOptions options)
      throws NotFoundException {
    MasterInfo master = mMasters.getFirstByField(ID_INDEX, masterId);
    if (master == null) {
      throw new NotFoundException(
          MessageFormat.format("No master with masterId {0,number,#} is found", masterId));
    }

    master.updateLastUpdatedTimeMs();

    mMasterConfigStore.registerNewConf(master.getAddress(), options.getConfigsList());

    LOG.info("registerMaster(): master: {}", master);
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.META_MASTER;
  }

  @Override
  public String getClusterID() {
    return mState.getClusterID();
  }

  @Override
  public CloseableIterator<Journal.JournalEntry> getJournalEntryIterator() {
    return CloseableIterator.concat(mPathProperties.getJournalEntryIterator(),
        mState.getJournalEntryIterator());
  }

  @Override
  public boolean processJournalEntry(Journal.JournalEntry entry) {
    return mState.processJournalEntry(entry) || mPathProperties.processJournalEntry(entry);
  }

  @Override
  public void resetState() {
    mState.resetState();
    mPathProperties.resetState();
  }

  @Override
  public Map<String, Boolean> updateConfiguration(Map<String, String> propertiesMap) {
    Map<String, Boolean> result = new HashMap<>();
    int successCount = 0;
    for (Map.Entry<String, String> entry : propertiesMap.entrySet()) {
      try {
        PropertyKey key = PropertyKey.fromString(entry.getKey());
        if (Configuration.getBoolean(PropertyKey.CONF_DYNAMIC_UPDATE_ENABLED)
            && key.isDynamic()) {
          Object oldValue = Configuration.get(key);
          Object value = key.parseValue(entry.getValue());
          Configuration.set(key, value, Source.RUNTIME);
          result.put(entry.getKey(), true);
          successCount++;
          LOG.info("Property {} has been updated to \"{}\" from \"{}\"",
              key.getName(), entry.getValue(), oldValue);
        } else {
          LOG.warn("Update a non-dynamic property {} is not allowed", key.getName());
          result.put(entry.getKey(), false);
        }
      } catch (Exception e) {
        result.put(entry.getKey(), false);
        LOG.error("Failed to update property {} to {}", entry.getKey(), entry.getValue(), e);
      }
    }
    LOG.debug("Update {} properties, succeed {}.", propertiesMap.size(), successCount);
    return result;
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
      long masterTimeoutMs = Configuration.getMs(PropertyKey.MASTER_HEARTBEAT_TIMEOUT);
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
   * Periodically log the config check report.
   */
  private final class LogConfigReportHeartbeatExecutor implements HeartbeatExecutor {
    private volatile boolean mFirst = true;

    @Override
    public void heartbeat() {
      // Skip the first heartbeat since it happens before servers have time to register their
      // configurations.
      if (mFirst) {
        mFirst = false;
      } else {
        mConfigChecker.logConfigReport();
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }
}
