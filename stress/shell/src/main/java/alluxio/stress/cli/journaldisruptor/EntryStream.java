package alluxio.stress.cli.journaldisruptor;

import alluxio.proto.journal.Journal;

import java.io.Closeable;
import java.io.IOException;

/**
 * the abstract class of EntryStream, can be implemented for both Ufs Journal and Raft Journal
 * Has two method, nextEntry will return next Alluxio entry
 * null when the stream comes the end. Close is decoration, do nothing now.
 */
public abstract class EntryStream implements Closeable {

  protected final String mMaster;
  protected final long mStart;
  protected final long mEnd;
  protected final String mInputDir;

  /**
   * init EntryStream.
   * @param master
   * @param start
   * @param end
   * @param inputDir
   */
  public EntryStream(String master, long start, long end, String inputDir) {
    mMaster = master;
    mStart = start;
    mEnd = end;
    mInputDir = inputDir;
  }

  /**
   * return one journal entry and one step forward.
   * @return next Alluxio journal entry
   */
  public abstract Journal.JournalEntry nextEntry();

  @Override
  public void close() throws IOException {}
}
