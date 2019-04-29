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

package alluxio.master.journal;

import alluxio.AlluxioURI;
import alluxio.RuntimeConstants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.NoopMaster;
import alluxio.master.journal.JournalReader.State;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CompoundCheckpointFormat.CompoundCheckpointReader;
import alluxio.master.journal.checkpoint.CompoundCheckpointFormat.CompoundCheckpointReader.Entry;
import alluxio.master.journal.checkpoint.TarballCheckpointFormat.TarballCheckpointReader;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsJournalReader;
import alluxio.master.journal.ufs.UfsJournalSystem;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.io.PathUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Tool for converting a ufs journal to a human-readable format.
 *
 * <pre>
 * java -cp \
 *   assembly/server/target/alluxio-assembly-server-<ALLUXIO-VERSION>-jar-with-dependencies.jar \
 *   alluxio.master.journal.JournalTool -master FileSystemMaster -outputDir my-journal
 * </pre>
 */
@NotThreadSafe
public final class JournalTool {
  private static final Logger LOG = LoggerFactory.getLogger(JournalTool.class);
  /** Separator to place at the end of each journal entry. */
  private static final String ENTRY_SEPARATOR = StringUtils.repeat('-', 80);
  private static final int EXIT_FAILED = -1;
  private static final int EXIT_SUCCEEDED = 0;
  private static final Options OPTIONS = new Options()
      .addOption("help", false, "Show help for this command.")
      .addOption("master", true,
          "The name of the master (e.g. FileSystemMaster, BlockMaster). "
              + "Set to FileSystemMaster by default.")
      .addOption("start", true,
          "The start log sequence number (inclusive). Set to 0 by default.")
      .addOption("end", true,
          "The end log sequence number (exclusive). Set to +inf by default.")
      .addOption("outputDir", true,
          "The output directory to write journal content to. Default: journal_dump-${timestamp}");

  private static boolean sHelp;
  private static String sMaster;
  private static long sStart;
  private static long sEnd;
  private static String sOutputDir;

  private JournalTool() {} // prevent instantiation

  /**
   * Dumps a ufs journal in human-readable format.
   *
   * @param args arguments passed to the tool
   */
  public static void main(String[] args) throws IOException {
    if (!parseInputArgs(args)) {
      usage();
      System.exit(EXIT_FAILED);
    }
    if (sHelp) {
      usage();
      System.exit(EXIT_SUCCEEDED);
    }

    dumpJournal();
  }

  private static void dumpJournal() throws IOException {
    JournalType journalType =
        ServerConfiguration.getEnum(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.class);

    AbstractJournalDumper journalDumper;
    switch (journalType) {
      case UFS:
        journalDumper = new UfsJournalDumper(sMaster, sStart, sEnd);
        break;
      case EMBEDDED:
        journalDumper = null;
      default:
        System.err.printf("Unsupported journal type: %s\n", journalType.name());
        return;
    }

    System.out.printf("Dumping journal of type %s to %s\n", journalType.name(), sOutputDir);
    // Ensure output directory structure.
    Files.createDirectories(Paths.get(sOutputDir));
    // Dump journal.
    journalDumper.dumpJournal(sOutputDir);
  }

  /**
   * Parses the input args with a command line format, using
   * {@link org.apache.commons.cli.CommandLineParser}.
   *
   * @param args the input args
   * @return true if parsing succeeded
   */
  private static boolean parseInputArgs(String[] args) {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(OPTIONS, args);
    } catch (ParseException e) {
      System.out.println("Failed to parse input args: " + e);
      return false;
    }
    sHelp = cmd.hasOption("help");
    sMaster = cmd.getOptionValue("master", "FileSystemMaster");
    sStart = Long.decode(cmd.getOptionValue("start", "0"));
    sEnd = Long.decode(cmd.getOptionValue("end", Long.valueOf(Long.MAX_VALUE).toString()));
    sOutputDir =
        new File(cmd.getOptionValue("outputDir", "journal_dump-" + System.currentTimeMillis()))
            .getAbsolutePath();
    return true;
  }

  /**
   * Prints the usage.
   */
  private static void usage() {
    new HelpFormatter().printHelp(
        "java -cp alluxio-" + RuntimeConstants.VERSION
            + "-jar-with-dependencies.jar alluxio.master.journal.JournalTool",
        "Read an Alluxio journal and write it to a directory in a human-readable format.", OPTIONS,
        "", true);
  }

  /**
   * An abstract class for journal dumpers.
   */
  static abstract class AbstractJournalDumper {
    protected final String mMaster;
    protected final long mStart;
    protected final long mEnd;

    /**
     * Creates abstract dumper with parameters.
     *
     * @param master journal master
     * @param start journal start sequence
     * @param end journal end sequence
     */
    public AbstractJournalDumper(String master, long start, long end) {
      mMaster = master;
      mStart = start;
      mEnd = end;
    }

    /**
     * Dumps journal.
     *
     * @param outputDir output directory for dump
     */
    abstract void dumpJournal(String outputDir);
  }

  /**
   * Used to dump journal of type UFS.
   */
  static class UfsJournalDumper extends AbstractJournalDumper {

    /**
     * Creates UFS journal dumper.
     *
     * @param master journal master
     * @param start journal start sequence number
     * @param end journal end sequence number
     */
    public UfsJournalDumper(String master, long start, long end) {
      super(master, start, end);
    }

    @Override
    public void dumpJournal(String outputDir) {
      String checkpointsDir = PathUtils.concatPath(outputDir, "checkpoints");
      String journalEntryFile = PathUtils.concatPath(outputDir, "edits.txt");

      UfsJournal journal =
          new UfsJournalSystem(getJournalLocation(), 0).createJournal(new NoopMaster(sMaster));
      try (
          PrintStream out =
              new PrintStream(new BufferedOutputStream(new FileOutputStream(journalEntryFile)));
          JournalReader reader = new UfsJournalReader(journal, sStart, true)) {
        boolean done = false;
        while (!done && reader.getNextSequenceNumber() < sEnd) {
          State state = reader.advance();
          switch (state) {
            case CHECKPOINT:
              try (CheckpointInputStream checkpoint = reader.getCheckpoint()) {
                Path dir = Paths.get(checkpointsDir + "-" + reader.getNextSequenceNumber());
                Files.createDirectories(dir);
                readCheckpoint(checkpoint, dir);
              }
              break;
            case LOG:
              JournalEntry entry = reader.getEntry();
              out.println(ENTRY_SEPARATOR);
              out.print(entry);
              break;
            case DONE:
              done = true;
              break;
            default:
              throw new RuntimeException("Unknown state: " + state);
          }
        }
      } catch (Exception e) {
        LOG.error("Failed to read from journal.", e);
      }
    }

    /**
     * @return the journal location
     */
    private URI getJournalLocation() {
      String journalDirectory = ServerConfiguration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
      if (!journalDirectory.endsWith(AlluxioURI.SEPARATOR)) {
        journalDirectory += AlluxioURI.SEPARATOR;
      }
      try {
        return new URI(journalDirectory);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    private void readCheckpoint(CheckpointInputStream checkpoint, Path path) throws IOException {
      LOG.debug("Reading checkpoint of type %s to %s%n", checkpoint.getType().name(), path);
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
      CompoundCheckpointReader reader = new CompoundCheckpointReader(checkpoint);
      Optional<Entry> entryOpt;
      while ((entryOpt = reader.nextCheckpoint()).isPresent()) {
        Entry entry = entryOpt.get();
        Path checkpointPath = path.resolve(entry.getName().toString());
        LOG.debug("Reading checkpoint for %s to %s%n", entry.getName(), checkpointPath);
        readCheckpoint(entry.getStream(), checkpointPath);
      }
    }

    private void readRocksCheckpoint(CheckpointInputStream checkpoint, Path path)
        throws IOException {
      TarballCheckpointReader reader = new TarballCheckpointReader(checkpoint);
      reader.unpackToDirectory(path);
    }

    private void readRegularCheckpoint(CheckpointInputStream checkpoint, Path path)
        throws IOException {
      try (PrintStream out =
          new PrintStream(new BufferedOutputStream(new FileOutputStream(path.toFile())))) {
        checkpoint.getType().getCheckpointFormat().parseToHumanReadable(checkpoint, out);
      }
    }
  }
}
