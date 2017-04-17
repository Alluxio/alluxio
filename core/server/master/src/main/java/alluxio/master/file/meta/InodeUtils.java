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
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.journal.JournalContext;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.MkdirsOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Utility methods for inodes.
 */
public final class InodeUtils {
  private static final Logger LOG = LoggerFactory.getLogger(InodeUtils.class);
  /** The base amount (exponential backoff) to sleep before retrying persisting an inode. */
  private static final int PERSIST_WAIT_BASE_SLEEP_MS = 2;
  /** Maximum amount (exponential backoff) to sleep before retrying persisting an inode. */
  private static final int PERSIST_WAIT_MAX_SLEEP_MS = 1000;
  /** The maximum retries for persisting an inode. */
  private static final int PERSIST_WAIT_MAX_RETRIES = 50;

  private InodeUtils() {} // prevent instantiation

  /**
   * Synchronously persists an {@link InodeDirectory} to the UFS. If concurrent calls are made, only
   * one thread will persist to UFS, and the others will wait until it is persisted.
   *
   * @param dir the {@link InodeDirectory} to persist
   * @param inodeTree the {@link InodeTree}
   * @param mountTable the {@link MountTable}
   * @param journalContext the journal context
   * @throws IOException if the file fails to persist
   * @throws InvalidPathException if the path for the inode is invalid
   * @throws FileDoesNotExistException if the path for the inode is invalid
   */
  public static void syncPersistDirectory(InodeDirectory dir, InodeTree inodeTree,
      MountTable mountTable, JournalContext journalContext) throws IOException,
      InvalidPathException, FileDoesNotExistException {
    RetryPolicy retry =
        new ExponentialBackoffRetry(PERSIST_WAIT_BASE_SLEEP_MS, PERSIST_WAIT_MAX_SLEEP_MS,
            PERSIST_WAIT_MAX_RETRIES);
    while (dir.getPersistenceState() != PersistenceState.PERSISTED) {
      if (dir.compareAndSwap(PersistenceState.NOT_PERSISTED,
          PersistenceState.TO_BE_PERSISTED)) {
        boolean success = false;
        StringBuilder errorDetails = new StringBuilder("Inode: ").append(dir.toString());
        try {
          AlluxioURI uri = inodeTree.getPath(dir);
          MountTable.Resolution resolution = mountTable.resolve(uri);
          String ufsUri = resolution.getUri().toString();
          errorDetails.append(" ufsUri: ").append(ufsUri);
          UnderFileSystem ufs = resolution.getUfs();
          MkdirsOptions mkdirsOptions =
              MkdirsOptions.defaults().setCreateParent(false).setOwner(dir.getOwner())
                  .setGroup(dir.getGroup()).setMode(new Mode(dir.getMode()));
          ufs.mkdirs(ufsUri, mkdirsOptions);
          dir.setPersistenceState(PersistenceState.PERSISTED);

          // Append the persist entry to the journal.
          File.PersistDirectoryEntry persistDirectory =
              File.PersistDirectoryEntry.newBuilder().setId(dir.getId()).build();
          journalContext.append(
              Journal.JournalEntry.newBuilder().setPersistDirectory(persistDirectory).build());
          success = true;
        } finally {
          if (!success) {
            // Failed to persist the inode, so set the state back to NOT_PERSISTED.
            dir.setPersistenceState(PersistenceState.NOT_PERSISTED);
          }
        }
      } else {
        if (!retry.attemptRetry()) {
          throw new IOException(ExceptionMessage.FAILED_UFS_CREATE.getMessage(dir.getName()));
        }
      }
    }
  }
}
