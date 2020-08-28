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

package alluxio.master.journal.tool;

import alluxio.master.file.meta.InodeView;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CompoundCheckpointFormat;
import alluxio.master.metastore.rocks.RocksInodeStore;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * An abstract class for journal dumpers.
 */
public abstract class AbstractJournalDumper {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractJournalDumper.class);

  protected final String mMaster;
  protected final long mStart;
  protected final long mEnd;
  protected final String mInputDir;
  protected final String mOutputDir;
  protected final String mCheckpointsDir;
  protected final String mJournalEntryFile;

  /**
   * @param master journal master
   * @param start journal start sequence
   * @param end journal end sequence
   * @param outputDir output dir for journal dump
   * @param inputDir input dir for journal files
   */
  public AbstractJournalDumper(String master, long start, long end, String outputDir,
      String inputDir) throws IOException {
    mMaster = master;
    mStart = start;
    mEnd = end;
    mInputDir = inputDir;
    mOutputDir = outputDir;
    mCheckpointsDir = PathUtils.concatPath(outputDir, "checkpoints");
    mJournalEntryFile = PathUtils.concatPath(outputDir, "edits.txt");

    // Ensure output directory.
    Files.createDirectories(Paths.get(mOutputDir));
  }

  /**
   * Dumps journal.
   */
  abstract void dumpJournal() throws Throwable;

  /**
   * Used to read checkpoint streams.
   *
   * @param checkpoint the checkpoint stream
   * @param path persistence path
   * @throws IOException
   */
  protected void readCheckpoint(CheckpointInputStream checkpoint, Path path) throws IOException {
    LOG.debug("Reading checkpoint of type {} to {}", checkpoint.getType().name(), path);
    switch (checkpoint.getType()) {
      case COMPOUND:
        readCompoundCheckpoint(checkpoint, path);
        break;
      case ROCKS:
        readRocksCheckpoint(checkpoint, path);
        break;
      default:
        readRegularCheckpoint(checkpoint, path);
        break;
    }
  }

  private void readCompoundCheckpoint(CheckpointInputStream checkpoint, Path path)
      throws IOException {
    Files.createDirectories(path);
    CompoundCheckpointFormat.CompoundCheckpointReader reader =
        new CompoundCheckpointFormat.CompoundCheckpointReader(checkpoint);
    Optional<CompoundCheckpointFormat.CompoundCheckpointReader.Entry> entryOpt;
    while ((entryOpt = reader.nextCheckpoint()).isPresent()) {
      CompoundCheckpointFormat.CompoundCheckpointReader.Entry entry = entryOpt.get();
      Path checkpointPath = path.resolve(entry.getName().toString());
      LOG.debug("Reading checkpoint for {} to {}", entry.getName(), checkpointPath);
      readCheckpoint(entry.getStream(), checkpointPath);
    }
  }

  private void readRocksCheckpoint(CheckpointInputStream checkpoint, Path path) throws IOException {
    // An empty dir for storing the db.
    Path dbPath = Paths.get(path.toFile().getPath() + "-rocks-db");
    // Create RocksInodeStore over checkpoint stream for extracting the inodes.
    try (PrintStream out =
             new PrintStream(new BufferedOutputStream(new FileOutputStream(path.toFile())));
         RocksInodeStore inodeStore = new RocksInodeStore(dbPath.toAbsolutePath().toString())) {
      // Create and restore RocksInodeStore from the checkpoint.
      inodeStore.restoreFromCheckpoint(checkpoint);
      // Dump entries.
      final String ENTRY_SEPARATOR = Strings.repeat("-", 80);
      for (InodeView inode : (Iterable<InodeView>) () -> inodeStore.iterator()) {
        out.println(ENTRY_SEPARATOR);
        out.println(inode.toProto());
      }
    } finally {
      // Remove the temp db directory.
      FileUtils.deletePathRecursively(dbPath.toFile().getPath());
    }
  }

  private void readRegularCheckpoint(CheckpointInputStream checkpoint, Path path)
      throws IOException {
    try (PrintStream out =
        new PrintStream(new BufferedOutputStream(new FileOutputStream(path.toFile())))) {
      checkpoint.getType().getCheckpointFormat().parseToHumanReadable(checkpoint, out);
    }
  }
}
