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
import alluxio.Constants;
import alluxio.ProcessUtils;
import alluxio.RuntimeConstants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.NoopMaster;
import alluxio.master.journal.JournalReader.State;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CompoundCheckpointFormat.CompoundCheckpointReader;
import alluxio.master.journal.checkpoint.CompoundCheckpointFormat.CompoundCheckpointReader.Entry;
import alluxio.master.journal.checkpoint.TarballCheckpointFormat.TarballCheckpointReader;
import alluxio.master.journal.raft.JournalEntryCommand;
import alluxio.master.journal.raft.OnceSupplier;
import alluxio.master.journal.raft.RaftJournalConfiguration;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.master.journal.raft.SnapshotReaderStream;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsJournalReader;
import alluxio.master.journal.ufs.UfsJournalSystem;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.atomix.catalyst.concurrent.SingleThreadContext;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.Command;
import io.atomix.copycat.protocol.ClientRequestTypeResolver;
import io.atomix.copycat.protocol.ClientResponseTypeResolver;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.copycat.server.storage.entry.CommandEntry;
import io.atomix.copycat.server.storage.snapshot.Snapshot;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;
import io.atomix.copycat.server.storage.util.StorageSerialization;
import io.atomix.copycat.server.util.ServerSerialization;
import io.atomix.copycat.util.ProtocolSerialization;
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
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Tool for converting journal to a human-readable format.
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
      .addOption("inputDir", true,
          "The input directory to read journal content from.")
      .addOption("outputDir", true,
          "The output directory to write journal content to. Default: journal_dump-${timestamp}");

  private static boolean sHelp;
  private static String sMaster;
  private static long sStart;
  private static long sEnd;
  private static String sInputDir;
  private static String sOutputDir;

  private JournalTool() {} // prevent instantiation

  /**
   * Dumps a ufs journal in human-readable format.
   *
   * @param args arguments passed to the tool
   */
  public static void main(String[] args) throws Throwable {
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

  @SuppressFBWarnings(value = "DB_DUPLICATE_SWITCH_CLAUSES")
  private static void dumpJournal() throws Throwable {
    JournalType journalType =
        ServerConfiguration.getEnum(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.class);

    AbstractJournalDumper journalDumper;
    switch (journalType) {
      case UFS:
        journalDumper = new UfsJournalDumper(sMaster, sStart, sEnd, sOutputDir, sInputDir);
        break;
      case EMBEDDED:
        journalDumper = new RaftJournalDumper(sMaster, sStart, sEnd, sOutputDir, sInputDir);
        break;
      default:
        System.err.println(String.format("Unsupported journal type: %s", journalType.name()));
        return;
    }

    System.out.println(
        String.format("Dumping journal of type %s to %s", journalType.name(), sOutputDir));
    journalDumper.dumpJournal();
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
    String inputDirOption = cmd.getOptionValue("inputDir", null);
    if (inputDirOption != null) {
      sInputDir = new File(inputDirOption).getAbsolutePath();
    }
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
  abstract static class AbstractJournalDumper {
    protected final String mMaster;
    protected final long mStart;
    protected final long mEnd;
    protected final String mInputDir;
    protected final String mOutputDir;
    protected final String mCheckpointsDir;
    protected final String mJournalEntryFile;

    /**
     * Creates abstract dumper with parameters.
     *
     * @param master journal master
     * @param start journal start sequence
     * @param end journal end sequence
     * @param outputDir output dir for journal dump
     * @param inputDir [optional] input dir for journal files
     */
    public AbstractJournalDumper(String master, long start, long end, String outputDir,
        @Nullable String inputDir) throws IOException {
      mMaster = master;
      mStart = start;
      mEnd = end;
      mInputDir = inputDir;
      mOutputDir = outputDir;
      mCheckpointsDir = PathUtils.concatPath(outputDir, "checkpoints");
      mJournalEntryFile = PathUtils.concatPath(outputDir, "edits.txt");

      // Ensure output directory structure.
      Files.createDirectories(Paths.get(mOutputDir));
      Files.createDirectories(Paths.get(mCheckpointsDir));
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
     * @param outputDir output dir for journal dump
     * @param inputDir [optional] input dir for journal files
     */
    public UfsJournalDumper(String master, long start, long end, String outputDir,
        @Nullable String inputDir) throws IOException {
      super(master, start, end, outputDir, inputDir);
    }

    @Override
    public void dumpJournal() throws Throwable {
      UfsJournal journal = new UfsJournalSystem(getJournalLocation(mInputDir), 0)
          .createJournal(new NoopMaster(sMaster));
      try (
          PrintStream out =
              new PrintStream(new BufferedOutputStream(new FileOutputStream(mJournalEntryFile)));
          JournalReader reader = new UfsJournalReader(journal, sStart, true)) {
        boolean done = false;
        while (!done && reader.getNextSequenceNumber() < sEnd) {
          State state = reader.advance();
          switch (state) {
            case CHECKPOINT:
              try (CheckpointInputStream checkpoint = reader.getCheckpoint()) {
                Path dir = Paths.get(mCheckpointsDir + "-" + reader.getNextSequenceNumber());
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
      }
    }

    /**
     * @return the journal location
     */
    private URI getJournalLocation(String inputDir) {
      // Read from configuration if not provided.
      if (inputDir == null) {
        String journalDirectory = ServerConfiguration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
        if (!journalDirectory.endsWith(AlluxioURI.SEPARATOR)) {
          journalDirectory += AlluxioURI.SEPARATOR;
        }
        inputDir = journalDirectory;
      }
      try {
        return new URI(inputDir);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static class RaftJournalDumper extends AbstractJournalDumper {
    /** Used to stop listening state form CopyCat cluster. */
    protected final AtomicLong mLastDumpOperationTime;
    /** Timeout to stop listening state from CopyCat cluster. */
    private static final int COPYCAT_SILENT_PERIOD_IN_SECONDS = 30;
    /** Timeout in seconds for joining/leaving a CopyCat cluster. */
    private static final int COPYCAT_CONNECTION_TIMEOUT_SECONDS = 30;
    /** Timeout in seconds for joining/leaving a CopyCat cluster. */
    private static final int COPYCAT_SNAPSHOT_READ_TIMEOUT_SECONDS = 3600;

    /**
     * Creates embedded journal dumper.
     *
     * @param master journal master
     * @param start journal start sequence
     * @param end journal end sequence
     * @param outputDir output dir for journal dump
     * @param inputDir [optional] input dir for journal files
     */
    public RaftJournalDumper(String master, long start, long end, String outputDir,
        @Nullable String inputDir) throws IOException {
      super(master, start, end, outputDir, inputDir);
      /*
       * We reset dump time whenever, - Writing to a snapshot is complete. - New JournalEntry
       * command is written. So, starting the last dump time from snapshot wait time will ensure,
       * the tool wait at most COPYCAT_SNAPSHOT_READ_TIMEOUT_SECONDS for snapshot, if there is any
       * snapshot.
       */
      mLastDumpOperationTime =
          new AtomicLong(System.currentTimeMillis() + COPYCAT_SNAPSHOT_READ_TIMEOUT_SECONDS * 1000);
    }

    @Override
    void dumpJournal() throws Throwable {
      RaftJournalConfiguration conf = RaftJournalConfiguration.defaults(getRaftServiceType());
      /*
       * Reading embedded journal actively requires a RAFT cluster with at least 1 follower node.
       * This is due to CopyCat allowing passive membership only through followers. In that case we
       * need to read from the journal directory supplied by the user. Since there is only one
       * master, the on-disk state will provide a consistent view.
       */
      if (mInputDir == null && conf.getClusterAddresses().size() == 1) {
        System.err.println("Cannot do live reading of embedded journal when "
            + "there is only one configured journal master.");
        System.err.println(
            "Please specify -inputDir option reading embedded journal files from disk");
        return;
      }

      // If provided, read from disk and return.
      if (mInputDir != null) {
        readFromDir();
        return;
      }

      // Attach to CopyCat cluster passively and dump state.
      try (PrintStream out =
          new PrintStream(new BufferedOutputStream(new FileOutputStream(mJournalEntryFile)))) {
        ServerSocket socket = new ServerSocket(0);
        int port = socket.getLocalPort();
        CopycatServer server = CopycatServer
            .builder(new Address(NetworkAddressUtils.getConnectHost(
                NetworkAddressUtils.ServiceType.MASTER_RAFT, ServerConfiguration.global()), port))
            .withSnapshotAllowed(new AtomicBoolean(false)).withTransport(new NettyTransport())
            .withSerializer(RaftJournalSystem.createSerializer()).withType(Member.Type.PASSIVE)
            .withStorage(Storage.builder().withStorageLevel(StorageLevel.MEMORY).build())
            .withStateMachine(new OnceSupplier<>(new EchoJournalStateMachine(
                (je) -> dumpEntry(je, out), this::dumpCheckpointStream)))
            .build();

        server
            .join(RaftJournalConfiguration.defaults(getRaftServiceType()).getClusterAddresses()
                .stream().map(addr -> new Address(addr.getHostName(), addr.getPort()))
                .collect(Collectors.toList()))
            .get(COPYCAT_CONNECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // Whether to stop listening for state due to silent period.
        while ((System.currentTimeMillis()
            - mLastDumpOperationTime.get() < COPYCAT_SILENT_PERIOD_IN_SECONDS
                * Constants.SECOND_MS)) {
          CommonUtils.sleepMs(100);
        }

        server.leave().get(COPYCAT_CONNECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }
    }

    private void dumpEntry(JournalEntry je, PrintStream out) {
      writeSelected(out, je);
      mLastDumpOperationTime.set(System.currentTimeMillis());
    }

    private void dumpCheckpointStream(CheckpointInputStream checkpointStream) {
      try {
        Path snapshotDir = Paths.get(mCheckpointsDir, "copycat-snapshot.log");
        readCheckpoint(checkpointStream, snapshotDir.toAbsolutePath());
      } catch (IOException exc) {
        LOG.error("Failed while reading checkpoint stream.", exc);
      } finally {
        mLastDumpOperationTime.set(System.currentTimeMillis());
      }
    }

    /**
     * Reads from the journal directly instead of going through the raft cluster. The state read
     * this way may be stale, but it can still be useful for debugging while the cluster is offline.
     */
    private void readFromDir() throws Throwable {
      Serializer serializer = RaftJournalSystem.createSerializer();
      serializer.resolve(new ClientRequestTypeResolver());
      serializer.resolve(new ClientResponseTypeResolver());
      serializer.resolve(new ProtocolSerialization());
      serializer.resolve(new ServerSerialization());
      serializer.resolve(new StorageSerialization());

      SingleThreadContext context = new SingleThreadContext("readJournal", serializer);

      try {
        // Read through the whole journal content, starting from snapshot.
        context.execute(this::readCopycatSnapshotFromDir).get();
        context.execute(this::readCopycatLogFromDir).get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw e;
      } catch (ExecutionException e) {
        throw e.getCause();
      }
    }

    private void readCopycatLogFromDir() {
      try (PrintStream out =
          new PrintStream(new BufferedOutputStream(new FileOutputStream(mJournalEntryFile)))) {
        Log log = Storage.builder().withDirectory(mInputDir).build().openLog("copycat");
        for (long i = log.firstIndex(); i < log.lastIndex(); i++) {
          io.atomix.copycat.server.storage.entry.Entry entry = log.get(i);
          if (entry instanceof CommandEntry) {
            Command command = ((CommandEntry) entry).getCommand();
            if (command instanceof JournalEntryCommand) {
              byte[] entryBytes = ((JournalEntryCommand) command).getSerializedJournalEntry();
              try {
                writeSelected(out, JournalEntry.parseFrom(entryBytes));
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
          }
        }
      } catch (Exception e) {
        LOG.error("Failed to read from journal.", e);
      }
    }

    private void readCopycatSnapshotFromDir() {
      Storage journalStorage = Storage.builder().withDirectory(mInputDir).build();
      if (journalStorage.openSnapshotStore("copycat").snapshots().isEmpty()) {
        LOG.debug("No snapshot found.");
        return;
      }
      Snapshot currentSnapshot = journalStorage.openSnapshotStore("copycat").currentSnapshot();

      String snapshotDumpFile =
          String.format("copycat-%s-%s.log", currentSnapshot.index(), currentSnapshot.timestamp());

      try (CheckpointInputStream checkpointStream =
          new CheckpointInputStream(new SnapshotReaderStream(currentSnapshot.reader()))) {
        readCheckpoint(checkpointStream, Paths.get(mCheckpointsDir, snapshotDumpFile));
      } catch (Exception e) {
        LOG.error("Failed to read from journal.", e);
      }
    }

    private NetworkAddressUtils.ServiceType getRaftServiceType() {
      if (mMaster.contains("job")) {
        return NetworkAddressUtils.ServiceType.JOB_MASTER_RAFT;
      } else {
        return NetworkAddressUtils.ServiceType.MASTER_RAFT;
      }
    }

    /**
     * Writes given entry after goin through range and validity checks.
     *
     * @param out out stream to write the entry to
     * @param entry the entry to write to
     */
    private void writeSelected(PrintStream out, JournalEntry entry) {
      if (entry == null) {
        return;
      }
      Preconditions.checkState(
          entry.getAllFields().size() <= 1
              || (entry.getAllFields().size() == 2 && entry.hasSequenceNumber()),
          "Raft journal entries should never set multiple fields in addition to sequence "
              + "number, but found %s",
          entry);
      if (entry.getJournalEntriesCount() > 0) {
        // This entry aggregates multiple entries.
        for (JournalEntry e : entry.getJournalEntriesList()) {
          writeSelected(out, e);
        }
      } else if (entry.toBuilder().clearSequenceNumber().build()
          .equals(JournalEntry.getDefaultInstance())) {
        // Ignore empty entries, they are created during snapshotting.
      } else {
        if (isSelected(entry)) {
          out.println(entry);
        }
      }
    }

    /**
     * Whether an entry is within the range of provided parameters to the tool.
     *
     * @param entry the journal entry
     * @return {@code true} if the entry should be included in the journal log
     */
    private boolean isSelected(JournalEntry entry) {
      long sn = entry.getSequenceNumber();
      if (sn >= mStart && sn < mEnd) {
        try {
          return JournalEntryAssociation.getMasterForEntry(entry).equalsIgnoreCase(mMaster);
        } catch (IllegalStateException e) {
          return false;
        }
      }
      return false;
    }

    /**
     * Implementation of {@link StateMachine} that echoes its state for dumping.
     *
     * EchoJournalStateMachine is attached to the cluster as PASSIVE, which makes it receive all
     * snapshots happening in the FOLLOWER node that it attaches to. For practical purposes, we
     * will dump the first snapshot only and stop dumping snapshots afterwards.
     */
    public static class EchoJournalStateMachine extends StateMachine implements Snapshottable {
      /** Used to consume JournalEntry commands coming to this state machine. */
      private final Consumer<JournalEntry> mEntryConsumer;
      /** Used to consume Snapshots streams coming to this state machine. */
      private final Consumer<CheckpointInputStream> mCheckpointConsumer;
      /** If the first snapshot is dumped. */
      private boolean mFirstSnapshotRead = false;

      /**
       * Creates a new echo state for dumping embedded journal.
       *
       * @param entryConsumer consumer for JournalEntry commands
       * @param checkpointConsumer consumer for Snapshot streams
       */
      public EchoJournalStateMachine(Consumer<JournalEntry> entryConsumer,
          Consumer<CheckpointInputStream> checkpointConsumer) {
        mEntryConsumer = entryConsumer;
        mCheckpointConsumer = checkpointConsumer;
      }

      /**
       * Applies a journal entry commit to the state machine.
       *
       * This method is automatically discovered by the Copycat framework.
       *
       * @param commit the commit
       */
      public synchronized void applyJournalEntryCommand(Commit<JournalEntryCommand> commit) {
        JournalEntry entry;
        try {
          entry = JournalEntry.parseFrom(commit.command().getSerializedJournalEntry());
        } catch (Throwable t) {
          ProcessUtils.fatalError(LOG, t, "Encountered invalid journal entry in commit: {}.",
              commit);
          throw new IllegalStateException();
        }
        mEntryConsumer.accept(entry);
      }

      @Override
      public void snapshot(SnapshotWriter writer) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void install(SnapshotReader snapshotReader) {
        if (mFirstSnapshotRead) {
          LOG.debug("Skip installing snapshot: {}", snapshotReader.readLong());
          return;
        }
        // Don't read snapshots coming after this one.
        mFirstSnapshotRead = true;
        LOG.debug("Installing snapshot: {}", snapshotReader.readLong());
        // Reading the snapshot.
        try (CheckpointInputStream checkpointStream =
            new CheckpointInputStream(new SnapshotReaderStream(snapshotReader))) {
          mCheckpointConsumer.accept(checkpointStream);
        } catch (Exception exc) {
          LOG.error(exc.toString());
        }
      }
    }
  }
}
