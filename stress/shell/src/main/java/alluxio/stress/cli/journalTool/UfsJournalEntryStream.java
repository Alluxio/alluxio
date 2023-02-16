package alluxio.stress.cli.journalTool;

import alluxio.AlluxioURI;
import alluxio.master.NoopMaster;
import alluxio.master.journal.JournalReader;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsJournalFile;
import alluxio.master.journal.ufs.UfsJournalReader;
import alluxio.master.journal.ufs.UfsJournalSystem;
import alluxio.proto.journal.Journal;

import java.net.URI;
import java.net.URISyntaxException;

public class UfsJournalEntryStream extends EntryStream {
  private boolean done = false;
  private UfsJournal journal;
  private JournalReader reader;

  public UfsJournalEntryStream(String master, long start, long end, String inputDir) {
    super(master, start, end, inputDir);
    journal = new UfsJournalSystem(getJournalLocation(mInputDir), 0).createJournal(new NoopMaster(mMaster));
    reader = new UfsJournalReader(journal, mStart, true);
    System.out.println(journal);
    System.out.println(getJournalLocation(mInputDir));
  }

  @Override
  public Journal.JournalEntry nextEntry() {
    if (reader.getNextSequenceNumber() < mEnd) {
      try{
        JournalReader.State state = reader.advance();
        switch (state) {
          case CHECKPOINT:
            // for now don't want to work with checkpoint now
            break;
          case LOG:
            return reader.getEntry();
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