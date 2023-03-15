package alluxio.master.file.meta;

public class InodeIterationResult {
  private final Inode mInode;

  private final String mName;

  public InodeIterationResult(Inode inode, String name) {
    this.mInode = inode;
    this.mName = name;
  }

  public Inode getInode() {
    return mInode;
  }

  public String getName() {
    return mName;
  }
}
