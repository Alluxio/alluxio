package alluxio.master.file.state;

/**
 * Class representing a directory ID. A directory ID has two parts; a container ID and a sequence
 * number.
 */
public class DirectoryId {
  private long mContainerId;
  private long mSequenceNumber;

  private final UnmodifiableDirectoryId mImmutableView;

  /**
   * Creates a new DirectoryId starting at (0, 0).
   */
  public DirectoryId() {
    mContainerId = 0;
    mSequenceNumber = 0;
    mImmutableView = new UnmodifiableDirectoryId() {
      @Override
      public long getContainerId() {
        return mContainerId;
      }

      @Override
      public long getSequenceNumber() {
        return mSequenceNumber;
      }
    };
  }

  /**
   * @return the container ID
   */
  public long getContainerId() {
    return mContainerId;
  }

  /**
   * @param containerId the container ID to set
   */
  public void setContainerId(long containerId) {
    mContainerId = containerId;
  }

  /**
   * @return the sequence number
   */
  public long getSequenceNumber() {
    return mSequenceNumber;
  }

  /**
   * @param sequenceNumber the sequence number to set
   */
  public void setSequenceNumber(long sequenceNumber) {
    mSequenceNumber = sequenceNumber;
  }

  /** Immutable view of a DirectoryId. */
  public interface UnmodifiableDirectoryId {
    /**
     * @return the container ID
     */
    long getContainerId();

    /**
     * @return the sequence number
     */
    long getSequenceNumber();
  }

  /**
   * @return an unmodifiable view of the DirectoryId
   */
  public UnmodifiableDirectoryId getImmutableView() {
    return mImmutableView;
  }
}
