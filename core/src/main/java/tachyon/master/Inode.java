package tachyon.master;

import tachyon.thrift.ClientFileInfo;

/**
 * <code>Inode</code> is an abstract class, with information shared by all types of Inodes.
 */
public abstract class Inode extends ImageWriter implements Comparable<Inode> {
  private final long mCreationTimeMs;
  protected final boolean mIsFolder;

  private int mId;
  private String mName;
  private int mParentId;

  /**
   * A pinned file is never evicted from memory. Folders are not pinned in memory; however, new
   * files and folders will inherit this flag from their parents.
   */
  private boolean mPinned = false;

  private long mLastModificationTimeMs;

  /**
   * Create an inode.
   * 
   * @param name the name of the inode.
   * @param id the id of the inode, which is globaly unique.
   * @param parentId the parent of the inode. -1 if there is no parent.
   * @param isFolder if the inode presents a folder
   * @param creationTimeMs the creation time of the inode.
   */
  protected Inode(String name, int id, int parentId, boolean isFolder, long creationTimeMs) {
    mCreationTimeMs = creationTimeMs;
    mIsFolder = isFolder;

    mId = id;
    mName = name;
    mParentId = parentId;
    mLastModificationTimeMs = creationTimeMs;
  }

  @Override
  public synchronized int compareTo(Inode o) {
    return mId - o.mId;
  }

  @Override
  public synchronized boolean equals(Object o) {
    if (!(o instanceof Inode)) {
      return false;
    }
    return mId == ((Inode) o).mId;
  }

  /**
   * Generate a ClientFileInfo of the file or folder.
   * 
   * @param path The path of the file
   * @return
   */
  public abstract ClientFileInfo generateClientFileInfo(String path);

  /**
   * Get the create time of the inode.
   * 
   * @return the create time, in milliseconds
   */
  public long getCreationTimeMs() {
    return mCreationTimeMs;
  }

  /**
   * Get the id of the inode
   * 
   * @return the id of the inode
   */
  public synchronized int getId() {
    return mId;
  }

  /**
   * Get the name of the inode
   * 
   * @return the name of the inode
   */
  public synchronized String getName() {
    return mName;
  }

  /**
   * Get the id of the parent folder
   * 
   * @return the id of the parent folder
   */
  public synchronized int getParentId() {
    return mParentId;
  }

  /**
   * Get the pinned flag of the inode
   * 
   * @return true if the inode is pinned, false otherwise
   */
  public synchronized boolean isPinned() {
    return mPinned;
  }

  /**
   * Get the last modification time of the inode
   * 
   * @return the last modification time, in milliseconds
   */
  public synchronized long getLastModificationTimeMs() {
    return mLastModificationTimeMs;
  }

  @Override
  public synchronized int hashCode() {
    return mId;
  }

  /**
   * Return whether the inode is a directory or not
   * 
   * @return true if the inode is a directory, false otherwise
   */
  public boolean isDirectory() {
    return mIsFolder;
  }

  /**
   * Return whether the inode is a file or not
   * 
   * @return true if the inode is a file, false otherwise
   */
  public boolean isFile() {
    return !mIsFolder;
  }

  /**
   * Reverse the id of the inode. Only used for a delete operation.
   */
  public synchronized void reverseId() {
    mId = -mId;
  }

  /**
   * Set the name of the inode
   * 
   * @param name The new name of the inode
   */
  public synchronized void setName(String name) {
    mName = name;
  }

  /**
   * Set the parent folder of the inode
   * 
   * @param parentId The new parent
   */
  public synchronized void setParentId(int parentId) {
    mParentId = parentId;
  }

  /**
   * Set the pinned flag of the inode
   * 
   * @param pinned If true, the inode need pinned, and a pinned file is never evicted from memory
   */
  public synchronized void setPinned(boolean pinned) {
    mPinned = pinned;
  }

  /**
   * Set the last modification time of the inode
   * 
   * @param lastModificationTimeMs The last modification time, in milliseconds
   */
  public synchronized void setLastModificationTimeMs(long lastModificationTimeMs) {
    mLastModificationTimeMs = lastModificationTimeMs;
  }

  @Override
  public synchronized String toString() {
    return new StringBuilder("Inode(").append("ID:").append(mId).append(", NAME:").append(mName)
        .append(", PARENT_ID:").append(mParentId).append(", CREATION_TIME_MS:")
        .append(mCreationTimeMs).append(", PINNED:").append(mPinned)
        .append(", LAST_MODIFICATION_TIME_MS:").append(mLastModificationTimeMs).append(")")
        .toString();
  }
}
