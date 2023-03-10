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

import org.apache.commons.io.FileUtils;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.impl.FileListSnapshotInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.util.MD5FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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

  private Matcher match(Path path) {
    return SimpleStateMachineStorage.SNAPSHOT_REGEX.matcher(path.getFileName().toString());
  }

  @Override
  public void init(RaftStorage raftStorage) throws IOException {
    mStorage = raftStorage;
    loadLatestSnapshot();
  }

  private SnapshotInfo findLatestSnapshot() {
    try (Stream<Path> stream = Files.list(getSnapshotDir().toPath())) {
      Optional<Path> max = stream.filter(path -> match(path).matches())
          .max(Comparator.comparingLong(path -> {
            TermIndex ti = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(path.toFile());
            return ti.getIndex();
          }));
      if (max.isPresent()) {
        TermIndex ti = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(max.get().toFile());
        List<FileInfo> fileInfos = new ArrayList<>();
        for (File file : FileUtils.listFiles(max.get().toFile(), null, true)) {
          MD5Hash md5Hash = MD5FileUtil.computeMd5ForFile(file);
          Path relativePath = max.get().relativize(file.toPath());
          fileInfos.add(new FileInfo(relativePath, md5Hash));
        }
        return new FileListSnapshotInfo(fileInfos, ti.getTerm(), ti.getIndex());
      }
    } catch (IOException e) {
      // do nothing and return null
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
  public void cleanupOldSnapshots(SnapshotRetentionPolicy retentionPolicy)
      throws IOException {
    if (!mNewSnapshotTaken) {
      LOG.debug("No new snapshot to delete old one");
      return;
    }
    mNewSnapshotTaken = false;
    try (Stream<Path> stream = Files.list(getSnapshotDir().toPath())) {
      stream.filter(path -> match(path).matches())
          .sorted(Comparator.comparingLong(path -> {
            TermIndex ti = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(path.toFile());
            // - to reverse the order
            return -ti.getIndex();
          }))
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
