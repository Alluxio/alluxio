package alluxio.stress.cli.journalTool;

import alluxio.proto.journal.Journal;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.master.journal.raft.RaftJournalUtils;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.raftlog.segmented.LogSegment;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogInputStream;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.StorageImplUtils;

import java.io.File;
import java.io.PrintStream;
import java.util.List;

public class RaftJournalEntryStream extends EntryStream {

  private List<LogSegmentPath> mPaths;
  private int mCurrentPathIndex = 0;
  private SegmentedRaftLogInputStream mIn = null;
  private RaftProtos.LogEntryProto mProto = null;

  public RaftJournalEntryStream(String inputDir) {
    super(inputDir);
    try (
        RaftStorage storage = StorageImplUtils.newRaftStorage(getJournalDir(),
        RaftServerConfigKeys.Log.CorruptionPolicy.getDefault(),
        RaftStorage.StartupOption.RECOVER,
        RaftServerConfigKeys.STORAGE_FREE_SPACE_MIN_DEFAULT.getSize())
        ) {
      storage.initialize();
      mPaths = LogSegmentPath.getLogSegmentPaths(storage);
    } catch (Exception e) {
      // do sth
    }
  }

  @Override
  public Journal.JournalEntry nextEntry() {
    // check current stream is ok, or open next stream. need a condition to stop and return null.
    if (mIn == null) {
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
        return processProto(mProto);
      } else {

      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return null;

    try {
      for (LogSegmentPath path: mPaths) {
        final int entryCount = LogSegment.readSegmentFile(path.getPath().toFile(),
            path.getStartEnd(), RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION,
            null, (proto) -> {
              if (proto.hasStateMachineLogEntry()) {
                try {
                  Journal.JournalEntry entry = Journal.JournalEntry.parseFrom(
                      proto.getStateMachineLogEntry().getLogData().asReadOnlyByteBuffer());
                } catch (Exception e) {

                }
              }
            });
      }
    } catch (Exception e) {
      // Do sth
    }

  }

  private Journal.JournalEntry processProto(RaftProtos.LogEntryProto proto) {
    if (proto.hasStateMachineLogEntry()) {
      try {
        Journal.JournalEntry entry = Journal.JournalEntry.parseFrom(
            proto.getStateMachineLogEntry().getLogData().asReadOnlyByteBuffer());
        return entry;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    // temporary
    throw new RuntimeException();
  }

  @Override
  public boolean checkNext() {
    return false;
  }

  private File getJournalDir() {
    return new File(RaftJournalUtils.getRaftJournalDir(new File(mInputDir)),
        RaftJournalSystem.RAFT_GROUP_UUID.toString());
  }
}
