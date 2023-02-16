package alluxio.stress.cli.journaldisruptor;

import alluxio.AlluxioURI;
import alluxio.master.NoopMaster;
import alluxio.master.journal.JournalReader;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsJournalReader;
import alluxio.master.journal.ufs.UfsJournalSystem;
import alluxio.proto.journal.Journal;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Ufs version of EntryStream.
 */
public class UfsJournalEntryStream extends EntryStream {
  private UfsJournal mJournal;
  private JournalReader mReader;

  /**
   * init UfsJournalEntryStream.
   * @param master
   * @param start
   * @param end
   * @param inputDir
   */
  public UfsJournalEntryStream(String master, long start, long end, String inputDir) {
    super(master, start, end, inputDir);
    mJournal = new UfsJournalSystem(
        getJournalLocation(mInputDir), 0).createJournal(new NoopMaster(mMaster));
    mReader = new UfsJournalReader(mJournal, mStart, true);
    System.out.println(mJournal);
    System.out.println(getJournalLocation(mInputDir));
  }

  @Override
  public Journal.JournalEntry nextEntry() {
    if (mReader.getNextSequenceNumber() < mEnd) {
      try {
        JournalReader.State state = mReader.advance();
        switch (state) {
          case CHECKPOINT:
            // for now don't want to work with checkpoint now
            break;
          case LOG:
            return mReader.getEntry();
          case DONE:
            return null;
          default:
            throw new RuntimeException("Unknown state: " + state);
        }
      } catch (Exception e) {
        // temp, need process this carefully
        throw new RuntimeException();
      }
    }
    throw new RuntimeException("SequenceNumber exceed");
  }

  private URI getJournalLocation(String inputDir) {
    if (!inputDir.endsWith(AlluxioURI.SEPARATOR)) {
      inputDir += AlluxioURI.SEPARATOR;
    }
    try {
      return new URI(inputDir);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
