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

package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.InvalidPathException;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.journal.DelegatingJournaled;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.Journaled;
import alluxio.proto.journal.File;
import alluxio.proto.journal.File.UpdateUfsModeEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.CloseableIterator;
import alluxio.util.network.NetworkAddressUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A class that manages the UFS for master servers.
 */
@ThreadSafe
public final class MasterUfsManager extends AbstractUfsManager implements DelegatingJournaled {
  private static final Logger LOG = LoggerFactory.getLogger(MasterUfsManager.class);

  private final State mState;

  /** A set of all managed ufs roots. */
  private final Set<String> mUfsRoots;

  /** Mapping from mount ID to ufs root. */
  private final Map<Long, String> mIdToRoot;

  /**
   * Constructs the instance of {@link MasterUfsManager}.
   */
  public MasterUfsManager() {
    mState = new State();
    mUfsRoots = new HashSet<>();
    mIdToRoot = new HashMap<>();
  }

  @Override
  protected void connectUfs(UnderFileSystem fs) throws IOException {
    fs.connectFromMaster(
        NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.MASTER_RPC,
            ServerConfiguration.global()));
  }

  @Override
  public synchronized void addMount(long mountId, final AlluxioURI ufsUri,
      final UnderFileSystemConfiguration ufsConf) {
    super.addMount(mountId, ufsUri, ufsConf);
    String root = ufsUri.getRootPath();
    mUfsRoots.add(root);
    mIdToRoot.put(mountId, root);
  }

  @Override
  public synchronized void removeMount(long mountId) {
    mIdToRoot.remove(mountId);
    super.removeMount(mountId);
  }

  /**
   * Get the physical ufs operation modes for the {@link UnderFileSystem} under the given Mount
   * table resolution.
   *
   * @param physicalStores the physical stores for the mount resolution
   * @return the state of physical UFS for given mount resolution
   */
  public synchronized Map<String, UfsMode> getPhysicalUfsState(
      List<String> physicalStores) {
    Map<String, UfsMode> ufsModeState = new HashMap<>();
    for (String physicalUfs : physicalStores) {
      ufsModeState.put(physicalUfs, mState.getUfsMode(new AlluxioURI(physicalUfs).getRootPath()));
    }
    return ufsModeState;
  }

  /**
   * Set the operation mode the given physical ufs.
   *
   * @param journalContext the journal context
   * @param ufsPath the physical ufs path (scheme and authority only)
   * @param ufsMode the ufs operation mode
   * @throws InvalidPathException if no managed ufs covers the given path
   */
  public synchronized void setUfsMode(Supplier<JournalContext> journalContext, AlluxioURI ufsPath,
      UfsMode ufsMode) throws InvalidPathException {
    LOG.info("Set ufs mode for {} to {}", ufsPath, ufsMode);

    String root = ufsPath.getRootPath();
    if (!mUfsRoots.contains(root)) {
      LOG.warn("No managed ufs for physical ufs path {}", root);
      throw new InvalidPathException(String.format("Unknown Ufs path %s", root));
    }

    mState.applyAndJournal(journalContext, UpdateUfsModeEntry.newBuilder()
        .setUfsPath(ufsPath.getRootPath())
        .setUfsMode(File.UfsMode.valueOf(ufsMode.name()))
        .build());
  }

  @Override
  public Journaled getDelegate() {
    return mState;
  }

  @Override
  public CloseableIterator<JournalEntry> getJournalEntryIterator() {
    return mState.getJournalEntryIterator();
  }

  private static class State implements Journaled {
    // The physical ufs state for all managed mounts. The keys are URIs normalized to set the path
    // to "/", e.g. "hdfs://namenode/" or just "/" for local filesystem.
    private final Map<String, UfsMode> mUfsModes = new HashMap<>();

    /**
     * @param key a root ufs path
     * @return the mode for that ufs
     */
    public UfsMode getUfsMode(String key) {
      return mUfsModes.getOrDefault(key, UfsMode.READ_WRITE);
    }

    @Override
    public void resetState() {
      mUfsModes.clear();
    }

    @Override
    public boolean processJournalEntry(JournalEntry entry) {
      if (entry.hasUpdateUfsMode()) {
        apply(entry.getUpdateUfsMode());
      } else {
        return false;
      }
      return true;
    }

    /**
     * @param context journal context
     * @param entry update ufs mode entry
     */
    public void applyAndJournal(Supplier<JournalContext> context, UpdateUfsModeEntry entry) {
      applyAndJournal(context, JournalEntry.newBuilder().setUpdateUfsMode(entry).build());
    }

    private void apply(UpdateUfsModeEntry entry) {
      mUfsModes.put(entry.getUfsPath(), UfsMode.valueOf(entry.getUfsMode().name()));
    }

    @Override
    public CloseableIterator<JournalEntry> getJournalEntryIterator() {
      return CloseableIterator.noopCloseable(mUfsModes.entrySet().stream()
          .map(e -> JournalEntry.newBuilder().setUpdateUfsMode(UpdateUfsModeEntry.newBuilder()
              .setUfsPath(e.getKey())
              .setUfsMode(File.UfsMode.valueOf(e.getValue().name())))
              .build())
          .iterator());
    }

    @Override
    public CheckpointName getCheckpointName() {
      return CheckpointName.MASTER_UFS_MANAGER;
    }
  }
}
