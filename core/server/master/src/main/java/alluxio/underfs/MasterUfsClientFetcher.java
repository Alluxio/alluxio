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
import alluxio.exception.InvalidPathException;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryIterable;
import alluxio.master.journal.JournalEntryReplayable;
import alluxio.proto.journal.File;
import alluxio.proto.journal.File.UpdateUfsModeEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.underfs.DefaultUfsClientCache.UfsClientFetcher;
import alluxio.worker.UfsClientCache.UfsClient;
import alluxio.underfs.UnderFileSystem.UfsMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A class that manages the UFS for master servers.
 */
@ThreadSafe
public final class MasterUfsClientFetcher
    implements UfsClientFetcher, JournalEntryIterable, JournalEntryReplayable {
  private static final Logger LOG = LoggerFactory.getLogger(MasterUfsClientFetcher.class);

  private final MountTable mMountTable;
  private final UfsCache mUfsCache;
  private final State mState;

  /**
   * Constructs the instance of {@link MasterUfsClientFetcher}.
   */
  public MasterUfsClientFetcher(MountTable mountTable, UfsCache ufsCache) {
    mMountTable = mountTable;
    mUfsCache = ufsCache;
    mState = new State();
  }

  /**
   * Get the physical ufs operation modes for the {@link UnderFileSystem} under the given Mount
   * table resolution.
   *
   * @param physicalStores the physical stores for the mount resolution
   * @return the state of physical UFS for given mount resolution
   */
  public synchronized Map<String, UnderFileSystem.UfsMode> getPhysicalUfsState(
      List<String> physicalStores) {
    Map<String, UnderFileSystem.UfsMode> ufsModeState = new HashMap<>();
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
    if (!mMountTable.containsUfsRoot(root)) {
      LOG.warn("No managed ufs for physical ufs path {}", root);
      throw new InvalidPathException(String.format("Unknown Ufs path %s", root));
    }

    mState.applyAndJournal(journalContext, UpdateUfsModeEntry.newBuilder()
        .setUfsPath(ufsPath.getRootPath())
        .setUfsMode(File.UfsMode.valueOf(ufsMode.name()))
        .build());
  }

  @Override
  public UfsClient getClient(long mountId) {
    MountInfo info = mMountTable.getMountInfo(mountId);
    return new UfsClient(
        () -> mUfsCache.getOrAdd(info.getAlluxioUri(), info.getOptions().toUfsConfiguration()),
        info.getUfsUri());
  }

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    return mState.getJournalEntryIterator();
  }

  @Override
  public boolean replayJournalEntryFromJournal(JournalEntry entry) {
    return mState.replayJournalEntryFromJournal(entry);
  }

  private static class State implements JournalEntryReplayable, JournalEntryIterable {
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
    public boolean replayJournalEntryFromJournal(JournalEntry entry) {
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
      apply(entry);
      context.get().append(JournalEntry.newBuilder().setUpdateUfsMode(entry).build());
    }

    private void apply(UpdateUfsModeEntry entry) {
      mUfsModes.put(entry.getUfsPath(), UfsMode.valueOf(entry.getUfsMode().name()));
    }

    @Override
    public Iterator<JournalEntry> getJournalEntryIterator() {
      return mUfsModes.entrySet().stream()
          .map(e -> JournalEntry.newBuilder().setUpdateUfsMode(UpdateUfsModeEntry.newBuilder()
              .setUfsPath(e.getKey())
              .setUfsMode(File.UfsMode.valueOf(e.getValue().name())))
              .build())
          .iterator();
    }
  }
}
