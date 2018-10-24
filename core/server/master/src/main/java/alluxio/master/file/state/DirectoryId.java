/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

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
  public UnmodifiableDirectoryId getUnmodifiableView() {
    return mImmutableView;
  }
}
