package tachyon.master;

import java.io.Serializable;

/**
 * Class to store global counter in master's write head log and checkpoint file.
 */
public class Counters implements Serializable, Comparable<Counters> {
  private static final long serialVersionUID = -8873733429025713755L;

  private int mInodeCounter;
  private long mEditTransactionId;
  private int mDependencyCounter;

  /**
   * Create a new Counters. It contains three counters, the counter for inode's id, the counter for
   * edit transaction's id, and the counter for dependency's id.
   * 
   * @param inodeCounter
   *          The initial value of the inodeCounter
   * @param editTransactionId
   *          The initial value of the editTransactionId
   * @param dependencyCounter
   *          The initial value of the dependencyCounter
   */
  public Counters(int inodeCounter, long editTransactionId, int dependencyCounter) {
    mInodeCounter = inodeCounter;
    mEditTransactionId = editTransactionId;
    mDependencyCounter = dependencyCounter;
  }

  @Override
  public synchronized int compareTo(Counters o) {
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
    if (!(o instanceof Counters)) {
      return false;
    }
    return compareTo((Counters) o) == 0;
  }

  public synchronized int getDependencyCounter() {
    return mDependencyCounter;
  }

  public synchronized long getEditTransactionCounter() {
    return mEditTransactionId;
  }

  public synchronized int getInodeCounter() {
    return mInodeCounter;
  }

  @Override
  public synchronized int hashCode() {
    return (int) (mInodeCounter + mEditTransactionId + mDependencyCounter);
  }

  /**
   * Update the dependencyCounter. Choose the maximum value between the current counter and the
   * parameter.
   * 
   * @param dependencyCounter
   *          The input dependencyCounter
   */
  public synchronized void updateDependencyCounter(int dependencyCounter) {
    mDependencyCounter = Math.max(mDependencyCounter, dependencyCounter);
  }

  /**
   * Update the editTransactionId. Choose the maximum value between the current counter and the
   * parameter.
   * 
   * @param id
   *          The input editTransactionId
   */
  public synchronized void updateEditTransactionCounter(long id) {
    mEditTransactionId = Math.max(mEditTransactionId, id);
  }

  /**
   * Update the inodeCounter. Choose the maximum value between the current counter and the
   * parameter.
   * 
   * @param inodeCounter
   *          The input inodeCounter
   */
  public synchronized void updateInodeCounter(int inodeCounter) {
    mInodeCounter = Math.max(mInodeCounter, inodeCounter);
  }

  @Override
  public String toString() {
    return new StringBuilder().append("Counter(").append(mInodeCounter).append(",")
        .append(mEditTransactionId).append(",").append(mDependencyCounter).append(")").toString();
  }
}