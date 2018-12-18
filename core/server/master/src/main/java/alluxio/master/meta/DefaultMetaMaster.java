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
import alluxio.Configuration;
import alluxio.ConfigurationValueOptions;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.Server;
import alluxio.clock.SystemClock;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.NotFoundException;
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
import alluxio.master.AbstractMaster;
import alluxio.master.MasterClientConfig;
import alluxio.master.MasterContext;
import alluxio.master.block.BlockMaster;
import alluxio.master.meta.checkconf.ServerConfigurationChecker;
import alluxio.master.meta.checkconf.ServerConfigurationStore;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.LockResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.ConfigurationUtils;
import alluxio.util.IdUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.Address;
import alluxio.wire.BackupOptions;
import alluxio.wire.BackupResponse;
import alluxio.wire.ConfigCheckReport;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
      = NetworkAddressUtils.getConnectAddress(NetworkAddressUtils.ServiceType.MASTER_RPC);

  /** The address of this master. */
  private Address mMasterAddress;

  /**
   * Creates a new instance of {@link DefaultMetaMaster}.
   *
   * @param blockMaster a block master handle
   * @param masterContext the context for Alluxio master
   */
  DefaultMetaMaster(BlockMaster blockMaster, MasterContext masterContext) {
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
  DefaultMetaMaster(BlockMaster blockMaster, MasterContext masterContext,
      ExecutorServiceFactory executorServiceFactory) {
    super(masterContext, new SystemClock(), executorServiceFactory);
    mMasterAddress =
        new Address().setHost(Configuration.getOrDefault(PropertyKey.MASTER_HOSTNAME, "localhost"))
            .setRpcPort(masterContext.getPort());
    mBlockMaster = blockMaster;
    mBlockMaster.registerLostWorkerFoundListener(mWorkerConfigStore::lostNodeFound);
    mBlockMaster.registerWorkerLostListener(mWorkerConfigStore::handleNodeLost);
    mBlockMaster.registerNewWorkerConfListener(mWorkerConfigStore::registerNewConf);
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    Map<ServiceType, GrpcService> services = new HashMap<>();
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
  public void processJournalEntry(JournalEntry entry) throws IOException {
    throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(entry));
  }

  @Override
  public void resetState() {}

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    return Iterators.unmodifiableIterator(Collections.emptyIterator());
  }

  @Override
  public void start(Boolean isPrimary) throws IOException {
    super.start(isPrimary);
    mWorkerConfigStore.reset();
    mMasterConfigStore.reset();
    if (isPrimary) {
      // Add the configuration of the current leader master
      mMasterConfigStore.registerNewConf(mMasterAddress,
          ConfigurationUtils.getConfiguration(Scope.MASTER));

      // The service that detects lost standby master nodes
      getExecutorService().submit(new HeartbeatThread(
          HeartbeatContext.MASTER_LOST_MASTER_DETECTION,
          new LostMasterDetectionHeartbeatExecutor(),
          (int) Configuration.getMs(PropertyKey.MASTER_MASTER_HEARTBEAT_INTERVAL)));
      getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_LOG_CONFIG_REPORT_SCHEDULING,
          new LogConfigReportHeartbeatExecutor(),
          (int) Configuration.getMs(PropertyKey.MASTER_LOG_CONFIG_REPORT_HEARTBEAT_INTERVAL)));
    } else {
      boolean haEnabled = Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED);
      if (haEnabled) {
        // Standby master should setup MetaMasterSync to communicate with the leader master
        RetryHandlingMetaMasterMasterClient metaMasterClient =
            new RetryHandlingMetaMasterMasterClient(MasterClientConfig.defaults());
        getExecutorService().submit(new HeartbeatThread(HeartbeatContext.META_MASTER_SYNC,
            new MetaMasterSync(mMasterAddress, metaMasterClient),
            (int) Configuration.getMs(PropertyKey.MASTER_MASTER_HEARTBEAT_INTERVAL)));
        LOG.info("Standby master with address {} starts sending heartbeat to leader master.",
            mMasterAddress);
      }
    }
  }

  @Override
  public BackupResponse backup(BackupOptions options) throws IOException {
    String dir = options.getTargetDirectory();
    if (dir == null) {
      dir = Configuration.get(PropertyKey.MASTER_BACKUP_DIRECTORY);
    }
    UnderFileSystem ufs;
    if (options.isLocalFileSystem()) {
      ufs = UnderFileSystem.Factory.create("/", UnderFileSystemConfiguration.defaults());
      LOG.info("Backing up to local filesystem in directory {}", dir);
    } else {
      ufs = UnderFileSystem.Factory.createForRoot();
      LOG.info("Backing up to root UFS in directory {}", dir);
    }
    if (!ufs.isDirectory(dir)) {
      if (!ufs.mkdirs(dir, MkdirsOptions.defaults().setCreateParent(true))) {
        throw new IOException(String.format("Failed to create directory %s", dir));
      }
    }
    String backupFilePath;
    try (LockResource lr = new LockResource(mMasterContext.pauseStateLock())) {
      Instant now = Instant.now();
      String backupFileName = String.format("alluxio-backup-%s-%s.gz",
          DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneId.of("UTC")).format(now),
          now.toEpochMilli());
      backupFilePath = PathUtils.concatPath(dir, backupFileName);
      try {
        try (OutputStream ufsStream = ufs.create(backupFilePath)) {
          mMasterContext.getBackupManager().backup(ufsStream);
        }
      } catch (Throwable t) {
        try {
          ufs.deleteFile(backupFilePath);
        } catch (Throwable t2) {
          LOG.error("Failed to clean up failed backup at {}", backupFilePath, t2);
          t.addSuppressed(t2);
        }
        throw t;
      }
    }
    String rootUfs = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    if (options.isLocalFileSystem()) {
      rootUfs = "file:///";
    }
    AlluxioURI backupUri = new AlluxioURI(new AlluxioURI(rootUfs), new AlluxioURI(backupFilePath));
    return new BackupResponse(backupUri,
        NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.MASTER_RPC));
  }

  @Override
  public ConfigCheckReport getConfigCheckReport() {
    return mConfigChecker.getConfigCheckReport();
  }

  @Override
  public List<ConfigProperty> getConfiguration(GetConfigurationPOptions options) {
    List<ConfigProperty> configInfoList = new ArrayList<>();
    for (PropertyKey key : Configuration.keySet()) {
      if (key.isBuiltIn()) {
        String source = Configuration.getSource(key).toString();
        String value = Configuration.getOrDefault(key, null, ConfigurationValueOptions.defaults()
            .useDisplayValue(true).useRawValue(options.getRawValue()));
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
    return mMasterContext.getStartTimeMs();
  }

  @Override
  public long getUptimeMs() {
    return System.currentTimeMillis() - mMasterContext.getStartTimeMs();
  }

  @Override
  public int getWebPort() {
    return Configuration.getInt(PropertyKey.MASTER_WEB_PORT);
  }

  @Override
  public boolean isInSafeMode() {
    return mMasterContext.getSafeModeManager().isInSafeMode();
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
