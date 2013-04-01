package tachyon;

import java.io.Serializable;

public class CheckpointInfo implements Serializable {
  private static final long serialVersionUID = -8873733429025713755L;

  public final int COUNTER_INODE;

  public CheckpointInfo(int inode) {
    COUNTER_INODE = inode;
  }
}