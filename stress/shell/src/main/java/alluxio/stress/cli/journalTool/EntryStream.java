package alluxio.stress.cli.journalTool;

import alluxio.proto.journal.Journal;

import java.io.Closeable;
import java.io.IOException;

public abstract class EntryStream implements Closeable {

  protected final String mMaster;
  protected final long mStart;
  protected final long mEnd;
  protected final String mInputDir;

  public EntryStream(String master, long start, long end, String inputDir) {
    mMaster = master;
    mStart = start;
    mEnd = end;
    mInputDir = inputDir;
  }

  /**
   * return one journal entry and one step forward
   */
  abstract public Journal.JournalEntry nextEntry();

  /**
   * @return whether next entry exist
   * true if exist
   * false if not exist
   */
  abstract public boolean checkNext();

  @Override
  public void close() throws IOException {

  }
}
