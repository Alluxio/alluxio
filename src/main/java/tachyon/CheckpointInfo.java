package tachyon;

import java.io.Serializable;

/**
 * Class to store global counter in master's write head log and checkpoint file.
 */
public class CheckpointInfo implements Serializable, Comparable<CheckpointInfo> {
  private static final long serialVersionUID = -8873733429025713755L;

  public final int COUNTER_INODE;

  public CheckpointInfo(int inodeCounter) {
    COUNTER_INODE = inodeCounter;
  }

  @Override
  public synchronized int compareTo(CheckpointInfo o) {
    return COUNTER_INODE - o.COUNTER_INODE;
  }

  @Override
  public synchronized boolean equals(Object o) {
    if (!(o instanceof CheckpointInfo)) {
      return false;
    }
    return COUNTER_INODE == ((CheckpointInfo)o).COUNTER_INODE;
  }

  @Override
  public synchronized int hashCode() {
    return COUNTER_INODE;
  }
}