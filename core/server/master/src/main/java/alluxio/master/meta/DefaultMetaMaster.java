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

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.Server;
import alluxio.clock.SystemClock;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.conf.ConfigurationValueOptions;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.Source;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.BackupPOptions;
import alluxio.grpc.ConfigProperties;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.GrpcService;
import alluxio.grpc.MetaCommand;
import alluxio.grpc.RegisterMasterPOptions;
import alluxio.grpc.Scope;
import alluxio.grpc.ServiceType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.BackupManager;
import alluxio.master.CoreMaster;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterClientContext;
import alluxio.master.block.BlockMaster;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.meta.checkconf.ServerConfigurationChecker;
import alluxio.master.meta.checkconf.ServerConfigurationStore;
import alluxio.proto.journal.Journal;
import alluxio.resource.LockResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.ConfigurationUtils;
import alluxio.util.IdUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.URIUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.Address;
import alluxio.wire.BackupResponse;
import alluxio.wire.ConfigCheckReport;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The default meta master.
 */
@NotThreadSafe
public final class DefaultMetaMaster extends CoreMaster implements MetaMaster {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetaMaster.class);
  private static final Set<Class<? extends Server>> DEPS =
      ImmutableSet.<Class<? extends Server>>of(BlockMaster.class);

  // Master metadata management.
  private static final IndexDefinition<MasterInfo, Long> ID_INDEX =
      new IndexDefinition<MasterInfo, Long>(true) {
        @Override
        public Long getFieldValue(MasterInfo o) {
          return o.getId();
        }
      };

  private static final IndexDefinition<MasterInfo, Address> ADDRESS_INDEX =
      new IndexDefinition<MasterInfo, Address>(true) {
        @Override
        public Address getFieldValue(MasterInfo o) {
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

  /** The connect address for the rpc server. */
  private final InetSocketAddress mRpcConnectAddress
      = NetworkAddressUtils.getConnectAddress(NetworkAddressUtils.ServiceType.MASTER_RPC,
      ServerConfiguration.global());

  /** The address of this master. */
  private Address mMasterAddress;

  /** The root ufs. */
  private final UnderFileSystem mUfs;

  /** The metadata daily backup. */
  private DailyMetadataBackup mDailyBackup;

  /** Path level properties. */
  private PathProperties mPathProperties;

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
    mMasterAddress =
        new Address().setHost(ServerConfiguration.getOrDefault(PropertyKey.MASTER_HOSTNAME,
            "localhost"))
            .setRpcPort(mPort);
    mBlockMaster = blockMaster;
    mBlockMaster.registerLostWorkerFoundListener(mWorkerConfigStore::lostNodeFound);
    mBlockMaster.registerWorkerLostListener(mWorkerConfigStore::handleNodeLost);
    mBlockMaster.registerNewWorkerConfListener(mWorkerConfigStore::registerNewConf);

    if (URIUtils.isLocalFilesystem(ServerConfiguration
        .get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS))) {
      mUfs = UnderFileSystem.Factory
          .create("/", UnderFileSystemConfiguration.defaults());
    } else {
      mUfs = UnderFileSystem.Factory.createForRoot(ServerConfiguration.global());
    }

    mPathProperties = new PathProperties();
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
          ConfigurationUtils.getConfiguration(ServerConfiguration.global(), Scope.MASTER));

      // The service that detects lost standby master nodes
      getExecutorService().submit(new HeartbeatThread(
          HeartbeatContext.MASTER_LOST_MASTER_DETECTION,
          new LostMasterDetectionHeartbeatExecutor(),
          (int) ServerConfiguration.getMs(PropertyKey.MASTER_MASTER_HEARTBEAT_INTERVAL),
          ServerConfiguration.global()));
      getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_LOG_CONFIG_REPORT_SCHEDULING,
          new LogConfigReportHeartbeatExecutor(),
          (int) ServerConfiguration.getMs(PropertyKey.MASTER_LOG_CONFIG_REPORT_HEARTBEAT_INTERVAL),
              ServerConfiguration.global()));

      if (ServerConfiguration.getBoolean(PropertyKey.MASTER_DAILY_BACKUP_ENABLED)) {
        mDailyBackup = new DailyMetadataBackup(this, Executors.newSingleThreadScheduledExecutor(
            ThreadFactoryUtils.build("DailyMetadataBackup-%d", true)), mUfs);
        mDailyBackup.start();
      }
    } else {
      if (ConfigurationUtils.isHaMode(ServerConfiguration.global())) {
        // Standby master should setup MetaMasterSync to communicate with the leader master
        RetryHandlingMetaMasterMasterClient metaMasterClient =
            new RetryHandlingMetaMasterMasterClient(MasterClientContext
                .newBuilder(ClientContext.create(ServerConfiguration.global())).build());
        getExecutorService().submit(new HeartbeatThread(HeartbeatContext.META_MASTER_SYNC,
            new MetaMasterSync(mMasterAddress, metaMasterClient),
            (int) ServerConfiguration.getMs(PropertyKey.MASTER_MASTER_HEARTBEAT_INTERVAL),
            ServerConfiguration.global()));
        LOG.info("Standby master with address {} starts sending heartbeat to leader master.",
            mMasterAddress);
      }
    }
  }

  @Override
  public void stop() throws IOException {
    if (mDailyBackup != null) {
      mDailyBackup.stop();
      mDailyBackup = null;
    }
    super.stop();
  }

  @Override
  public BackupResponse backup(BackupPOptions options) throws IOException {
    String dir = options.hasTargetDirectory() ? options.getTargetDirectory()
        : ServerConfiguration.get(PropertyKey.MASTER_BACKUP_DIRECTORY);
    UnderFileSystem ufs = mUfs;
    if (options.getLocalFileSystem() && !ufs.getUnderFSType().equals("local")) {
      ufs = UnderFileSystem.Factory.create("/", UnderFileSystemConfiguration.defaults());
      LOG.info("Backing up to local filesystem in directory {}", dir);
    } else {
      LOG.info("Backing up to root UFS in directory {}", dir);
    }
    if (!ufs.isDirectory(dir)) {
      if (!ufs.mkdirs(dir, MkdirsOptions.defaults(ServerConfiguration.global())
          .setCreateParent(true))) {
        throw new IOException(String.format("Failed to create directory %s", dir));
      }
    }
    String backupFilePath;
    try (LockResource lr = new LockResource(mMasterContext.pauseStateLock())) {
      Instant now = Instant.now();
      String backupFileName = String.format(BackupManager.BACKUP_FILE_FORMAT,
          DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneId.of("UTC")).format(now),
          now.toEpochMilli());
      backupFilePath = PathUtils.concatPath(dir, backupFileName);
      try {
        try (OutputStream ufsStream = ufs.create(backupFilePath)) {
          mBackupManager.backup(ufsStream);
        }
      } catch (Throwable t) {
        try {
          ufs.deleteExistingFile(backupFilePath);
        } catch (Throwable t2) {
          LOG.error("Failed to clean up failed backup at {}", backupFilePath, t2);
          t.addSuppressed(t2);
        }
        throw t;
      }
    }
    String rootUfs = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    if (options.getLocalFileSystem()) {
      rootUfs = "file:///";
    }
    AlluxioURI backupUri = new AlluxioURI(new AlluxioURI(rootUfs), new AlluxioURI(backupFilePath));
    return new BackupResponse(backupUri,
        NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.MASTER_RPC,
            ServerConfiguration.global()));
  }

  @Override
  public ConfigCheckReport getConfigCheckReport() {
    return mConfigChecker.getConfigCheckReport();
  }

  @Override
  public List<ConfigProperty> getConfiguration(GetConfigurationPOptions options) {
    List<ConfigProperty> configInfoList = new ArrayList<>();
    for (PropertyKey key : ServerConfiguration.keySet()) {
      if (key.isBuiltIn()) {
        String source = ServerConfiguration.getSource(key).toString();
        String value = ServerConfiguration.getOrDefault(key, null,
            ConfigurationValueOptions.defaults().useDisplayValue(true)
                .useRawValue(options.getRawValue()));
        ConfigProperty.Builder config =
            ConfigProperty.newBuilder().setName(key.getName()).setSource(source);
        if (value != null) {
          config.setValue(value);
        }
        configInfoList.add(config.build());
      }
    }
    return configInfoList;
  }

  @Override
  public Map<String, ConfigProperties> getPathConfiguration(GetConfigurationPOptions options) {
    Map<String, ConfigProperties> pathConfig = new HashMap<>();
    mPathProperties.get().forEach((path, properties) -> {
      List<ConfigProperty> configPropertyList = new ArrayList<>();
      properties.forEach((key, value) ->
          configPropertyList.add(ConfigProperty.newBuilder().setName(key)
              .setSource(Source.PATH_DEFAULT.toString()).setValue(value).build()));
      ConfigProperties configProperties = ConfigProperties.newBuilder().addAllProperties(
          configPropertyList).build();
      pathConfig.put(path, configProperties);
    });
    return pathConfig;
  }

  @Override
  public String getConfigurationVersion() {
    // TODO(cc)
    return null;
  }

  @Override
  public String getPathConfigurationVersion() {
    // TODO(cc)
    return null;
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
  public List<Address> getMasterAddresses() {
    return mMasterConfigStore.getLiveNodeAddresses();
  }

  @Override
  public List<Address> getWorkerAddresses() {
    return mWorkerConfigStore.getLiveNodeAddresses();
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
    return ServerConfiguration.getInt(PropertyKey.MASTER_WEB_PORT);
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
      throw new NotFoundException(ExceptionMessage.NO_MASTER_FOUND.getMessage(masterId));
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
  public Iterator<Journal.JournalEntry> getJournalEntryIterator() {
    return mPathProperties.getJournalEntryIterator();
  }

  @Override
  public boolean processJournalEntry(Journal.JournalEntry entry) {
    return mPathProperties.processJournalEntry(entry);
  }

  @Override
  public void resetState() {
    mPathProperties.resetState();
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
      long masterTimeoutMs = ServerConfiguration.getMs(PropertyKey.MASTER_HEARTBEAT_TIMEOUT);
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
