package alluxio.stress.cli.journalTool;

import alluxio.proto.journal.Journal;

public class JournalDisruptor {
  EntryStream mStream;
  long mDisruptStep;
  long mStepCounter;
  int mEntryType;
  Journal.JournalEntry mEntry;
  Journal.JournalEntry mHoldEntry;
  boolean mHoldFlag;
  long mHoldEntrySequenceNumber;

  public JournalDisruptor(EntryStream stream, long step, int type) {
    mStream = stream;
    mDisruptStep = step;
    mEntryType = type;
    mHoldFlag = false;
  }

  /**
   * For now the Disrupt() implements only a very simple behavior pattern:
   *  The Disruptor will put the entries with target entry-type back for given steps.
   *
   * I think the Disrupt cannot handle two target type entry within the given steps, it will only process the first one.
   */
  public void Disrupt() {
    while ((mEntry = mStream.nextEntry()) != null) {
      if (mHoldFlag) {
        if (mStepCounter == 0) {
          writeEntry(mHoldEntry.toBuilder().setSequenceNumber(mHoldEntrySequenceNumber).build());
          mHoldFlag = false;
          continue;
        }
        writeEntry(mEntry.toBuilder().setSequenceNumber(mHoldEntrySequenceNumber).build());
        mHoldEntrySequenceNumber += 1;
        mStepCounter -= 1;
        continue;
      }
      if (targetEntry(mEntry)) {
        mHoldFlag = true;
        mHoldEntrySequenceNumber = mEntry.getSequenceNumber();
        mHoldEntry = mEntry;
        mStepCounter = mDisruptStep;
        continue;
      }
      writeEntry(mEntry);
    }
  }

  /**
   * the targetEntry can only handle the hard coded entry type below
   * @param entry
   * @return true if the given entry has the target entry type
   */
  private boolean targetEntry(Journal.JournalEntry entry) {
    int type = -1;
    if (entry.hasDeleteFile()) {
      type = 0;
    } else if (entry.hasInodeDirectory()) {
      type = 1;
    } else if (entry.hasInodeFile()) {
      type = 2;
    } else if (entry.hasNewBlock()) {
      type = 3;
    } else if (entry.hasRename()) {
      type = 4;
    } else if (entry.hasSetAcl()) {
      type = 5;
    } else if (entry.hasUpdateInode()) {
      type = 6;
    } else if (entry.hasUpdateInodeDirectory()) {
      type = 7;
    } else if (entry.hasUpdateInodeFile()) {
      type = 8;
      // Deprecated entries
    } else if (entry.hasAsyncPersistRequest()) {
      type = 9;
    } else if (entry.hasCompleteFile()) {
      type = 10;
    } else if (entry.hasInodeLastModificationTime()) {
      type = 11;
    } else if (entry.hasPersistDirectory()) {
      type = 12;
    } else if (entry.hasSetAttribute()) {
      type = 13;
    } else if (entry.hasInodeDirectoryIdGenerator()) {
      type = 14;
    } else if (entry.hasDeleteFile()) {
      type = 15;
    } else {
      type = -1;
    }
    return type == mEntryType;
  }

  private void writeEntry(Journal.JournalEntry entry) {

  }
}
