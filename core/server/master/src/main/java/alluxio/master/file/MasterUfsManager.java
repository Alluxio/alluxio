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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.meta.MountTable;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryIterable;
import alluxio.master.journal.JournalEntryReplayable;
import alluxio.proto.journal.File;
import alluxio.proto.journal.File.UpdateUfsModeEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.underfs.UnderFileSystem.UfsMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Manages per-ufs metadata, such as whether the ufs has been set to read-only mode.
 */
public class MasterUfsManager implements JournalEntryReplayable, JournalEntryIterable {
  private static final Logger LOG = LoggerFactory.getLogger(MasterUfsManager.class);

  private final MountTable mMountTable;
  private final State mState = new State();

  /**
   * @param mountTable the mount table
   */
  public MasterUfsManager(MountTable mountTable) {
    mMountTable = mountTable;
  }

  /**
   * @param ufsUri a ufs uri
   * @return the ufs mode
   */
  public UfsMode getUfsMode(String ufsUri) {
    return mState.getMode(ufsUri);
  }

  /**
   * @param journalContext journal context
   * @param ufsUri uri indicating which ufs to set the mode for
   * @param mode the mode to set
   */
  public void setUfsMode(Supplier<JournalContext> journalContext, AlluxioURI ufsUri, UfsMode mode)
      throws InvalidPathException {
    String root = ufsUri.getRootPath();
    if (!mMountTable.containsUfsRoot(root)) {
      LOG.warn("No managed ufs for physical ufs path {}", root);
      throw new InvalidPathException(String.format("Unknown Ufs path %s", root));
    }
    mState.applyAndJournal(journalContext, UpdateUfsModeEntry.newBuilder()
        .setUfsPath(root)
        .setUfsMode(File.UfsMode.valueOf(mode.name()))
        .build());
  }

  @Override
  public boolean replayJournalEntryFromJournal(JournalEntry entry) {
    return mState.replayJournalEntryFromJournal(entry);
  }

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    return mState.getJournalEntryIterator();
  }

  @ThreadSafe
  private static class State implements JournalEntryReplayable, JournalEntryIterable {
    /**
     * Mapping from ufs roots to ufs modes. Ufs roots are URIs with only scheme and authority. Path is
     * always "/".
     */
    private final HashMap<String, UfsMode> mUfsModes = new HashMap<>();

    /**
     * @param key a root ufs path
     * @return the mode for that ufs
     */
    public synchronized UfsMode getMode(String key) {
      // TODO(andrew) normalize paths to make sure they are root URIs.
      return mUfsModes.getOrDefault(key, UfsMode.READ_WRITE);
    }

    @Override
    public synchronized boolean replayJournalEntryFromJournal(JournalEntry entry) {
      if (entry.hasUpdateUfsMode()) {
        apply(entry.getUpdateUfsMode());
      } else {
        return false;
      }
      return true;
    }

    /**
     * @param context journal context
     * @param entry   update ufs mode entry
     */
    public synchronized void applyAndJournal(Supplier<JournalContext> context, UpdateUfsModeEntry entry) {
      apply(entry);
      context.get().append(JournalEntry.newBuilder().setUpdateUfsMode(entry).build());
    }

    private void apply(UpdateUfsModeEntry entry) {
      mUfsModes.put(entry.getUfsPath(), UfsMode.valueOf(entry.getUfsMode().name()));
    }

    @Override
    public synchronized Iterator<JournalEntry> getJournalEntryIterator() {
      return mUfsModes.entrySet().stream()
          .map(e -> JournalEntry.newBuilder().setUpdateUfsMode(UpdateUfsModeEntry.newBuilder()
              .setUfsPath(e.getKey())
              .setUfsMode(File.UfsMode.valueOf(e.getValue().name())))
              .build())
          .iterator();
    }
  }
}
