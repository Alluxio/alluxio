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

package alluxio.master.journal.raft;

import alluxio.annotation.SuppressFBWarnings;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.NotFileFilter;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.impl.FileListSnapshotInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.MD5FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Simple state machine storage that can handle directories.
 */
public class SnapshotDirStateMachineStorage implements StateMachineStorage {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotDirStateMachineStorage.class);

  private RaftStorage mStorage;
  @Nullable
  private volatile SnapshotInfo mLatestSnapshotInfo = null;
  private volatile boolean mNewSnapshotTaken = false;

  private final Comparator<Path> mSnapshotPathComparator = Comparator.comparing(
      path -> SimpleStateMachineStorage.getTermIndexFromSnapshotFile(path.toFile()));

  /**
   * @param path to evaluate
   * @return a matcher to evaluate if the leaf of the provided path has a name that matches the
   * pattern of snapshot directories
   */
  @SuppressFBWarnings(value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE",
      justification = "argument 'path' is never null, and method 'matcher' returns NotNull")
  public static Matcher matchSnapshotPath(Path path) {
    return SimpleStateMachineStorage.SNAPSHOT_REGEX.matcher(path.getFileName().toString());
  }

  @Override
  public void init(RaftStorage raftStorage) throws IOException {
    mStorage = raftStorage;
    loadLatestSnapshot();
  }

  private SnapshotInfo findLatestSnapshot() {
    try (Stream<Path> stream = Files.list(getSnapshotDir().toPath())) {
      Optional<Path> max = stream
          .filter(path -> matchSnapshotPath(path).matches())
          .max(mSnapshotPathComparator);
      if (max.isPresent()) {
        TermIndex ti = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(max.get().toFile());
        // for backwards compatibility with previous versions of snapshots
        if (max.get().toFile().isFile()) {
          MD5Hash md5Hash = MD5FileUtil.readStoredMd5ForFile(max.get().toFile());
          FileInfo fileInfo = new FileInfo(max.get(), md5Hash);
          return new SingleFileSnapshotInfo(fileInfo, ti.getTerm(), ti.getIndex());
        }
        // new snapshot format
        List<FileInfo> fileInfos = new ArrayList<>();
        Collection<File> nonMd5Files = FileUtils.listFiles(max.get().toFile(),
            new NotFileFilter(new SuffixFileFilter(MD5FileUtil.MD5_SUFFIX)),
            TrueFileFilter.INSTANCE);
        for (File file : nonMd5Files) {
          MD5Hash md5Hash = MD5FileUtil.readStoredMd5ForFile(file); // null if no md5 file
          Path relativePath = max.get().relativize(file.toPath());
          fileInfos.add(new FileInfo(relativePath, md5Hash));
        }
        return new FileListSnapshotInfo(fileInfos, ti.getTerm(), ti.getIndex());
      }
    } catch (Exception e) {
      // Files.list may throw an unchecked exception
      // do nothing and return null
      LOG.warn("Error reading snapshot directory", e);
    }
    return null;
  }

  /**
   * Loads the latest snapshot information into the StateMachineStorage.
   */
  public void loadLatestSnapshot() {
    mLatestSnapshotInfo = findLatestSnapshot();
  }

  @Override @Nullable
  public SnapshotInfo getLatestSnapshot() {
    return mLatestSnapshotInfo;
  }

  @Override
  public void format() throws IOException {}

  /**
   * Signal to the StateMachineStorage that a new snapshot was taken.
   */
  public void signalNewSnapshot() {
    mNewSnapshotTaken = true;
  }

  @Override
  public void cleanupOldSnapshots(SnapshotRetentionPolicy retentionPolicy) throws IOException {
    if (!mNewSnapshotTaken) {
      LOG.trace("No new snapshot to delete old one");
      return;
    }
    mNewSnapshotTaken = false;
    try (Stream<Path> stream = Files.list(getSnapshotDir().toPath())) {
      stream.filter(path -> matchSnapshotPath(path).matches())
          .sorted(Collections.reverseOrder(mSnapshotPathComparator))
          .skip(retentionPolicy.getNumSnapshotsRetained())
          .forEach(path -> {
            LOG.debug("removing dir {}", path.getFileName());
            boolean b = FileUtils.deleteQuietly(path.toFile());
            LOG.debug("{}successful deletion", b ? "" : "un");
          });
    }
  }

  @Override
  public File getSnapshotDir() {
    return mStorage.getStorageDir().getStateMachineDir();
  }

  @Override
  public File getTmpDir() {
    return mStorage.getStorageDir().getTmpDir();
  }
}
