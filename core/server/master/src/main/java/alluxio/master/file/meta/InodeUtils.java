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

package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.master.journal.JournalEntryAppender;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.CommonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the tree of Inode's.
 */
public final class InodeUtils {
  private static final Logger LOG = LoggerFactory.getLogger(InodeUtils.class);

  private InodeUtils() {} // prevent instantiation

  /**
   * Synchronously persists an {@link InodeDirectory} to the UFS. If concurrent calls are made, only
   * one thread will persist to UFS, and the others will wait until it is persisted.
   *
   * @param dir the {@link InodeDirectory} to persist
   * @param inodeTree the {@link InodeTree}
   * @param mountTable the {@link MountTable}
   * @param journalAppender the appender to journal the persist entry to, if not null
   */
  public static void syncPersistDirectory(InodeDirectory dir, InodeTree inodeTree,
      MountTable mountTable, JournalEntryAppender journalAppender) {
    // TODO(gpang): use a max timeout.
    while (dir.getPersistenceState() != PersistenceState.PERSISTED) {
      if (dir.compareAndSwapPersistenceState(PersistenceState.NOT_PERSISTED,
          PersistenceState.TO_BE_PERSISTED)) {
        boolean success = false;
        try {
          AlluxioURI uri = inodeTree.getPath(dir);
          MountTable.Resolution resolution = mountTable.resolve(uri);
          String ufsUri = resolution.getUri().toString();
          UnderFileSystem ufs = resolution.getUfs();
          MkdirsOptions mkdirsOptions =
              MkdirsOptions.defaults().setCreateParent(false).setOwner(dir.getOwner())
                  .setGroup(dir.getGroup()).setMode(new Mode(dir.getMode()));
          ufs.mkdirs(ufsUri, mkdirsOptions);
          dir.setPersistenceState(PersistenceState.PERSISTED);

          if (journalAppender != null) {
            // Append the persist entry to the journal.
            File.PersistDirectoryEntry persistDirectory =
                File.PersistDirectoryEntry.newBuilder().setId(dir.getId()).build();
            journalAppender.append(
                Journal.JournalEntry.newBuilder().setPersistDirectory(persistDirectory).build());
          }
          success = true;
        } catch (Exception e) {
          // Ignore
        } finally {
          if (!success) {
            // Failed to persist the inode, so set the state back to NOT_PERSISTED.
            dir.setPersistenceState(PersistenceState.NOT_PERSISTED);
          }
        }
      } else {
        // TODO(gpang): use exponential backoff and max timeout
        CommonUtils.sleepMs(150);
      }
    }
  }
}
