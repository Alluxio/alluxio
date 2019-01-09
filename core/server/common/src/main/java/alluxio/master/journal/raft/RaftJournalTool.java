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

import alluxio.Constants;
import alluxio.ProcessUtils;
import alluxio.master.journal.JournalEntryStreamReader;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
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
import io.atomix.copycat.server.cluster.Member.Type;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.copycat.server.storage.entry.CommandEntry;
import io.atomix.copycat.server.storage.entry.Entry;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;
import io.atomix.copycat.server.storage.util.StorageSerialization;
import io.atomix.copycat.server.util.ServerSerialization;
import io.atomix.copycat.util.ProtocolSerialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Tool for reading a Raft journal.
 */
public final class RaftJournalTool {
  private static final Logger LOG = LoggerFactory.getLogger(RaftJournalTool.class);

  private static final String USAGE = "Reads a Raft journal. This requires a running Raft cluster "
      + "so as not to rely on the local log being authoritative. This tool connects to the Raft "
      + "cluster and prints all journal entries.";

  @Parameter(names = {"-type"},
      description = "The journal to print. Options are [Alluxio, Job]. Defaults to Alluxio")
  private String mType = "Alluxio";

  @Parameter(names = {"-dir"},
      description = "A journal directory to read. If set, the tool will read directly from "
          + "local journal files, not requiring Alluxio servers to be running.")
  private String mDir;

  @Parameter(names = {"-snapshot"},
      description = "Whether to read the snapshot file")
  private boolean mSnapshot = false;

  @Parameter(names = {"-h", "-help"}, help = true)
  private boolean mHelp = false;

  /**
   * @param args command line arguments
   */
  public static void main(String[] args) {
    System.exit(new RaftJournalTool().run(args));
  }

  /**
   * Prints usage.
   */
  private static void usage(JCommander jc) {
    System.out.println(USAGE);
    jc.usage();
  }

  /**
   * Constructs a new {@link RaftJournalTool}.
   */
  public RaftJournalTool() {}

  /**
   * @param args command line arguments
   * @return an exit status
   */
  public int run(String[] args) {
    JCommander jc = new JCommander(this);
    jc.setProgramName(getClass().getSimpleName());
    try {
      jc.parse(args);
    } catch (Exception e) {
      System.out.println(e.toString());
      System.out.println();
      usage(jc);
      return -1;
    }
    if (mHelp) {
      usage(jc);
      return 0;
    }
    if (mDir != null) {
      return readFromDir();
    }

    AtomicLong lastUpdate = new AtomicLong(-1);
    try {
      ServerSocket socket = new ServerSocket(0);
      int port = socket.getLocalPort();
      CopycatServer server = CopycatServer
          .builder(new Address(NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RAFT), port))
          .withSnapshotAllowed(new AtomicBoolean(false))
          .withTransport(new NettyTransport())
          .withSerializer(RaftJournalSystem.createSerializer())
          .withType(Type.PASSIVE)
          .withStorage(Storage.builder().withStorageLevel(StorageLevel.MEMORY).build())
          .withStateMachine(() -> new EchoJournalStateMachine(lastUpdate)).build();

      ServiceType serviceType =
          mType.equalsIgnoreCase("Alluxio") ? ServiceType.MASTER_RAFT : ServiceType.JOB_MASTER_RAFT;
      server.join(RaftJournalConfiguration.defaults(serviceType)
          .getClusterAddresses().stream()
          .map(addr -> new Address(addr.getHostName(), addr.getPort()))
          .collect(Collectors.toList()));
      while (lastUpdate.get() < 0
          || (System.currentTimeMillis() - lastUpdate.get() < Constants.SECOND_MS)) {
        CommonUtils.sleepMs(100);
      }
      server.leave().get();
    } catch (Exception e) {
      System.out.println("Failed to read journal");
      e.printStackTrace();
      System.exit(-1);
    }
    return 0;
  }

  /**
   * Reads from the journal directly instead of going through the raft cluster. The state read this
   * way may be stale, but it can still be useful for debugging while the cluster is offline.
   *
   * @return exit code
   */
  private int readFromDir() {
    Serializer serializer = RaftJournalSystem.createSerializer();
    serializer.resolve(new ClientRequestTypeResolver());
    serializer.resolve(new ClientResponseTypeResolver());
    serializer.resolve(new ProtocolSerialization());
    serializer.resolve(new ServerSerialization());
    serializer.resolve(new StorageSerialization());

    SingleThreadContext context =
        new SingleThreadContext("readJournal", serializer);
    try {
      if (mSnapshot) {
        context.execute(this::readSnapshot).get();
      } else {
        context.execute(this::readLog).get();
      }
    } catch (InterruptedException e) {
      return -1;
    } catch (ExecutionException e) {
      e.printStackTrace();
      return -1;
    }
    return 0;
  }

  private void readLog() {
    Log log = Storage.builder().withDirectory(mDir).build().openLog("copycat");
    for (long i = log.firstIndex(); i < log.lastIndex(); i++) {
      Entry entry = log.get(i);
      if (entry instanceof CommandEntry) {
        Command command = ((CommandEntry) entry).getCommand();
        if (command instanceof JournalEntryCommand) {
          byte[] entryBytes = ((JournalEntryCommand) command).getSerializedJournalEntry();
          try {
            System.out.println("Entry " + i + ": " + JournalEntry.parseFrom(entryBytes));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }

  private void readSnapshot() {
    try (SnapshotReader snapshotReader = Storage.builder().withDirectory(mDir).build()
        .openSnapshotStore("copycat").currentSnapshot().reader()) {
      JournalEntryStreamReader reader =
          new JournalEntryStreamReader(new SnapshotReaderStream(snapshotReader));
      while (snapshotReader.hasRemaining()) {
        try {
          System.out.println(reader.readEntry());
        } catch (IOException e) {
          throw new RuntimeException("Failed to read snapshot", e);
        }
      }
    }
  }

  private static class EchoJournalStateMachine extends StateMachine implements Snapshottable {
    private final AtomicLong mLastUpdate;

    public EchoJournalStateMachine(AtomicLong lastUpdate) {
      mLastUpdate = lastUpdate;
    }

    /**
     * Applies a journal entry commit to the state machine.
     *
     * This method is automatically discovered by the Copycat framework.
     *
     * @param commit the commit
     */
    public void applyJournalEntryCommand(Commit<JournalEntryCommand> commit) {
      JournalEntry entry;
      try {
        entry = JournalEntry.parseFrom(commit.command().getSerializedJournalEntry());
      } catch (Throwable t) {
        ProcessUtils.fatalError(LOG, t, "Encountered invalid journal entry in commit: {}.", commit);
        throw new IllegalStateException();
      }
      applyEntry(entry);
    }

    /**
     * Applies the journal entry, handling empty entries and multi-entries.
     *
     * @param entry the entry to apply
     */
    private void applyEntry(JournalEntry entry) {
      Preconditions.checkState(
          entry.getAllFields().size() <= 1
              || (entry.getAllFields().size() == 2 && entry.hasSequenceNumber()),
          "Raft journal entries should never set multiple fields in addition to sequence "
              + "number, but found %s",
          entry);
      if (entry.getJournalEntriesCount() > 0) {
        // This entry aggregates multiple entries.
        for (JournalEntry e : entry.getJournalEntriesList()) {
          applyEntry(e);
        }
      } else if (entry.toBuilder().clearSequenceNumber().build()
          .equals(JournalEntry.getDefaultInstance())) {
        // Ignore empty entries, they are created during snapshotting.
      } else {
        printEntry(entry);
      }
    }

    private void printEntry(JournalEntry entry) {
      mLastUpdate.set(System.currentTimeMillis());
      System.out.println(entry);
    }

    @Override
    public void snapshot(SnapshotWriter writer) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void install(SnapshotReader snapshotReader) {
      JournalEntryStreamReader reader =
          new JournalEntryStreamReader(new SnapshotReaderStream(snapshotReader));

      while (snapshotReader.hasRemaining()) {
        JournalEntry entry;
        try {
          entry = reader.readEntry();
        } catch (IOException e) {
          LOG.error("Failed to install snapshot", e);
          throw new RuntimeException(e);
        }
        printEntry(entry);
      }
    }
  }
}
