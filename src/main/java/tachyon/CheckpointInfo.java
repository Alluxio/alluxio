package tachyon;

import java.io.Serializable;

/**
 * Class to store global counter in master's write head log and checkpoint file.
 */
public class CheckpointInfo implements Serializable, Comparable<CheckpointInfo> {
  private static final long serialVersionUID = -8873733429025713755L;

  private int mInodeCounter;
  private long mEditTransactionId;
  private int mDependencyCounter;

  public CheckpointInfo(int inodeCounter, long editTransactionId, int dependencyCounter) {
    mInodeCounter = inodeCounter;
    mEditTransactionId = editTransactionId;
    mDependencyCounter = dependencyCounter;
  }

  public synchronized void updateInodeCounter(int inodeCounter) {
    mInodeCounter = Math.max(mInodeCounter, inodeCounter);
  }

  public synchronized void updateEditTransactionCounter(long id) {
    mEditTransactionId = Math.max(mEditTransactionId, id);
  }

  public synchronized void updateDependencyCounter(int dependencyCounter) {
    mDependencyCounter = Math.max(mDependencyCounter, dependencyCounter);
  }

  public synchronized int getInodeCounter() {
    return mInodeCounter;
  }

  public synchronized long getEditTransactionCounter() {
    return mEditTransactionId;
  }

  public synchronized int getDependencyCounter() {
    return mDependencyCounter;
  }

  @Override
  public synchronized int compareTo(CheckpointInfo o) {
    if (mInodeCounter != o.mInodeCounter) {
      return mInodeCounter - o.mInodeCounter;
    }
    if (mEditTransactionId != o.mEditTransactionId) {
      return mEditTransactionId > o.mEditTransactionId ? 1 : -1;
    }
    if (mDependencyCounter == o.mDependencyCounter) {
      return 0;
    }
    return mDependencyCounter > o.mDependencyCounter ? 1 : -1;
  }

  @Override
  public synchronized boolean equals(Object o) {
    if (!(o instanceof CheckpointInfo)) {
      return false;
    }
    return compareTo((CheckpointInfo)o) == 0;
  }

  @Override
  public synchronized int hashCode() {
    return (int) (mInodeCounter + mEditTransactionId + mDependencyCounter);
  }
}