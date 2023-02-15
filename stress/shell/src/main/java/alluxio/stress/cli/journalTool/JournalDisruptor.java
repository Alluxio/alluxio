package alluxio.stress.cli.journalTool;

import alluxio.master.block.BlockId;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;

public class JournalDisruptor {
  long mDisruptStep;
  JournalReader mReader;
  long mStepCounter;
  int mEntryType;
  Journal.JournalEntry mEntry;
  Journal.JournalEntry mHoldEntry;
  boolean mHoldFlag;
  long mHoldEntrySequenceNumber;

  public JournalDisruptor(JournalReader reader, long step, int type) {
    mReader = reader;
    mDisruptStep = step;
    mEntryType = type;
    mHoldFlag = false;
  }

  /**
   * For now the Disrupt() implements only a very simple behavior pattern:
   *  The Disruptor will put the entries with target entry-type back for given steps.
   *
   * I think the Disrupt cannot handle two target type entry within the given steps, it will only process the first one.
   *
   *  2.15 update:
   *    Now in my imagination the disruptor likes a gun, and the JournalReader likes the magazine
   */
  public void Disrupt() {
    while ((mEntry = mReader.nextEntry()) != null) {
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

  public Journal.JournalEntry nextEntry() {
    if (mHoldFlag && mStepCounter == 0) {
      mEntry = mHoldEntry;
      mHoldEntry = null;
      mHoldFlag = false;
      mStepCounter = mDisruptStep;
      return mEntry;
    }

    if ((mEntry = mReader.nextEntry()) != null) {
      if (mHoldFlag) {
        mStepCounter -= 1;
        return mEntry;
      }
      if (targetEntry(mEntry)) {
        mHoldFlag = true;
        // writer will set SN, no need to worry
        // mHoldEntrySequenceNumber = mEntry.getSequenceNumber();
        mHoldEntry = mEntry;
        return nextEntry();
      }
      return mEntry;
    }
    return mHoldEntry;
  }

  public Journal.JournalEntry test() {
    if ((mEntry = mReader.nextEntry()) != null) {
      if (targetEntry(mEntry)) {
        // return mEntry.toBuilder().setUpdateInode(File.UpdateInodeEntry.newBuilder().setId(BlockId.createBlockId(mEntry.getUpdateInode().getId()+100, BlockId.getMaxSequenceNumber())).build()).build();
        return mEntry.toBuilder().setUpdateInode(mEntry.getUpdateInode().toBuilder().setId(mEntry.getUpdateInode().getId()+100).build()).build();
      }
    }
    return mEntry;
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
