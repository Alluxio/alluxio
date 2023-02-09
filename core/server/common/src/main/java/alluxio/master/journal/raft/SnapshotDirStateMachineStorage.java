package alluxio.master.journal.raft;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.stream.Stream;

/**
 * Simple state machine storage that can handle directories.
 */
public class SnapshotDirStateMachineStorage extends SimpleStateMachineStorage {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotDirStateMachineStorage.class);
  private Matcher match(Path path) {
    return SNAPSHOT_REGEX.matcher(path.getFileName().toString());
  }

  // inspired by https://github.com/apache/ratis/blob/31efad1e973348f3a81ccf71523c1d85dce408ef/ratis-server/src/main/java/org/apache/ratis/statemachine/impl/SimpleStateMachineStorage.java#L78-L107
  @Override
  public void cleanupOldSnapshots(SnapshotRetentionPolicy retentionPolicy) throws IOException {
    try (Stream<Path> stream = Files.list(getSmDir().toPath())) {
      stream.map(path -> new ImmutablePair<>(path, match(path)))
          .filter(pair -> pair.getRight().matches())
          .map(pair -> {
            final long endIndex = Long.parseLong(pair.getRight().group(2));
            final long term = Long.parseLong(pair.getRight().group(1));
            final FileInfo fileInfo = new FileInfo(pair.getLeft(), null);
            LOG.info("found {}", pair.getLeft().getFileName());
            return new SingleFileSnapshotInfo(fileInfo, term, endIndex);
          })
          .sorted((file1, file2) -> (int) (file2.getIndex() - file1.getIndex()))
          .skip(retentionPolicy.getNumSnapshotsRetained())
          .map(snapshotFile -> snapshotFile.getFile().getPath().toFile())
          .forEach(file -> {
            LOG.info("removing dir {}", file.getName());
            boolean b = FileUtils.deleteQuietly(file);
            LOG.info("{}successful deletion", b ? "" : "un");
          });
    }
  }
}
