package alluxio.stress.cli.journalTool;

import alluxio.master.journal.JournalEntryStreamReader;
import alluxio.proto.journal.Journal;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.master.journal.raft.RaftJournalUtils;
import alluxio.util.proto.ProtoUtils;

// import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.raftlog.segmented.LogSegment;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogInputStream;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.StorageImplUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.Descriptors;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RaftJournalEntryStream extends EntryStream {

  private List<LogSegmentPath> mPaths;
  private int mCurrentPathIndex = 0;
  private SegmentedRaftLogInputStream mIn = null;
  private RaftProtos.LogEntryProto mProto = null;
  private InputStream mStream;
  private JournalEntryStreamReader mReader;
  private byte[] mBuffer = new byte[4096];

  private RaftProtos.LogEntryProto proto;
  private Journal.JournalEntry entry;
  private List<Journal.JournalEntry> list = new ArrayList<>();
  private int index = 0;

  public RaftJournalEntryStream(String master, long start, long end, String inputDir) {
    super(master, start, end, inputDir);
    try (
        RaftStorage storage = StorageImplUtils.newRaftStorage(getJournalDir(),
        RaftServerConfigKeys.Log.CorruptionPolicy.getDefault(),
        RaftStorage.StartupOption.RECOVER,
        RaftServerConfigKeys.STORAGE_FREE_SPACE_MIN_DEFAULT.getSize())
        ) {
      storage.initialize();
      System.out.printf("storage is: %s", storage);
      mPaths = LogSegmentPath.getLogSegmentPaths(storage);
    } catch (Exception e) {
      // do sth
      System.out.println(e);
    }
  }

  @Override
  public Journal.JournalEntry nextEntry() {
    System.out.println("nexting entry");
    if (index < list.size()) {
      return list.get(index++);
    }
    while ((proto = nextProto()) != null && !proto.hasStateMachineLogEntry()) {}
    if (proto == null) {
      // no proto, stream comes to the end
      return null;
    }
    try {
      entry = Journal.JournalEntry.parseFrom(proto.getStateMachineLogEntry().getLogData().asReadOnlyByteBuffer());
    } catch (Exception e) {
      System.out.println("failed to parse proto to entry: " + e);
    }
    list = entry.getJournalEntriesList();
    index = 0;
    return nextEntry();
  }

  /**
   * since the non-log entry proto needs to be process/print by the journal tool,
   * here cannot integrate the process proto
   * @return
   */
  @Override
  public RaftProtos.LogEntryProto nextProto() {
    // check current stream is ok, or open next stream. need a condition to stop and return null.
    if (mIn == null) {
      if (mCurrentPathIndex == mPaths.size()) {
        return null;
      }
      LogSegmentPath path = mPaths.get(mCurrentPathIndex);
      try {
        mIn = new SegmentedRaftLogInputStream(path.getPath().toFile(), path.getStartEnd().getStartIndex(), path.getStartEnd().getEndIndex(), path.getStartEnd().isOpen());
        mCurrentPathIndex += 1;
      } catch (Exception e) {
        // LOG sth, but how to init log with destination...
      }
    }

    try {
      mProto = mIn.nextEntry();
      if (mProto != null) {
        return mProto;
      } else {
        mIn = null;
        return nextProto();
      }
    } catch (Exception e) {
      System.out.println(e);
      throw new RuntimeException(e);
    }
  }

  /**
   * the func processProto will update EntryStream.mStream with given proto
   * if proto has the StateMachineLogEntry the mSteam will be updated successfully and the function will return true.
   * if proto doesn't have the StateMachineLogEntry the mSteam will not be changed and the function will return false.
   * @param proto
   * @return boolean
   */
  @Override
  public boolean processProto(RaftProtos.LogEntryProto proto) {
    System.out.print("processing proto!");
    if (proto.hasStateMachineLogEntry()) {
      // mStream = new ByteArrayInputStream(proto.getStateMachineLogEntry().getLogData().asReadOnlyByteBuffer().array());
      mStream = new ByteArrayInputStream(proto.toByteArray());
      // mStream = new ByteArrayInputStream(proto.getStateMachineLogEntry().toByteArray());
      mReader = new JournalEntryStreamReader(mStream);
      System.out.println("got a entry reader");
      try {
        Journal.JournalEntry entry = Journal.JournalEntry.parseFrom(proto.getStateMachineLogEntry().getLogData().asReadOnlyByteBuffer());
        Journal.JournalEntry tmp;
        int i = 0;
        while ((tmp = entry.getJournalEntries(i)) != null) {
          System.out.println(tmp);
          i += 1;
        }
        System.out.println();
      } catch (Exception e) {
        System.out.print(e);
      }
      return true;

    }
    System.out.print("fail to got a entry reader");
    return false;
  }

   public Journal.JournalEntry readEntry() throws IOException {
    int firstByte = mStream.read();
    if (firstByte == -1) {
      return null;
    }
    // All journal entries start with their size in bytes written as a varint.
    int size;
    try {
      size = ProtoUtils.readRawVarint32(firstByte, mStream);
    } catch (IOException e) {
      System.out.println(e);
      System.out.println("get byte with size from stream fail");
      throw e;
    }
    if (mBuffer.length < size) {
      mBuffer = new byte[size];
    }
    // Total bytes read so far for journal entry.
    int totalBytesRead = 0;
    while (totalBytesRead < size) {
      // Bytes read in last read request.
      int latestBytesRead = mStream.read(mBuffer, totalBytesRead, size - totalBytesRead);
      if (latestBytesRead < 0) {
        break;
      }
      totalBytesRead += latestBytesRead;
    }
    if (totalBytesRead < size) {
      // This could happen if the master crashed partway through writing the final journal entry. In
      // this case, we can ignore the last entry because it was not acked to the client.
      System.out.printf("Journal entry was truncated. Expected to read {} bytes but only got {}", size,
          totalBytesRead);
      return null;
    }
    return Journal.JournalEntry.parser().parseFrom(mBuffer, 0, size);
  }

  @Override
  public boolean checkNext() {
    return false;
  }

  private File getJournalDir() {
    return new File(RaftJournalUtils.getRaftJournalDir(new File(mInputDir)),
        RaftJournalSystem.RAFT_GROUP_UUID.toString());
  }

  @Override
  public void close() throws IOException {
    mReader.close();
  }
}
