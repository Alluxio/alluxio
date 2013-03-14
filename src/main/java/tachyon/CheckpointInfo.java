package tachyon;

import java.io.Serializable;

public class CheckpointInfo implements Serializable {
  private static final long serialVersionUID = -8873733429025713755L;

  public final int COUNTER_INODE;
  
  public final int COUNTER_DEPENDENCY;

  public CheckpointInfo(int inode, int dependency) {
    COUNTER_INODE = inode;
    COUNTER_DEPENDENCY = dependency;
  }
}