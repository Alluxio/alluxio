package alluxio.stress.cli.journalTool;

import alluxio.master.journal.JournalType;
import alluxio.master.journal.tool.AbstractJournalDumper;
import alluxio.master.journal.tool.UfsJournalDumper;
import alluxio.master.journal.tool.RaftJournalDumper;
import alluxio.proto.journal.Journal;

/**
 *  I assume here should use alluxio.master.journal.tool.AbstractJournalDumper.
 *  two Dumper: UFSJournalDumper/RaftJournalDumper, read and parse binary journal file from proto to JournalEntry.
 *  For now, I think JournalReader should be able to return something like JournalEntry stream.
 *  and should be able to be specified by path and journal type.
 *
 *  But do we have anything like EntryStream now?
 *
 *  TO_BE_DECIDED:
 *      1. read one journal file specified by given path or read all journal entries from both checkpoint and normal log.
 */

public class JournalReader {

  private String mJournalPath;
  private JournalType mJournalType;
  private EntryStream mStream;


  public JournalReader(String mMaster, long mStart, long mEnd, String mInputDir) {
    switch (mJournalType) {
      case UFS:
        mStream = new UfsJournalEntryStream(mMaster, mStart, mEnd, mInputDir);
        break;
      case EMBEDDED:
        mStream = new RaftJournalEntryStream(mMaster, mStart, mEnd, mInputDir);
        break;
      default:
        System.err.printf("Unsupported journal type: %s%n", mJournalType.name());
        return;
    }
  }

    public Journal.JournalEntry readJournal() {
      return mStream.nextEntry();
    }
}
