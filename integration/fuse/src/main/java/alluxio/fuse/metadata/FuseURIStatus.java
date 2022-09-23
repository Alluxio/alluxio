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

package alluxio.fuse.metadata;

import alluxio.client.file.URIStatus;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Represents the metadata about a file or directory used by Alluxio POSIX API.
 */
@ThreadSafe
public class FuseURIStatus {
  private final String mName;
  private final long mLength;
  private final boolean mCompleted;
  private final boolean mFolder;
  private final long mLastModificationTimeMs;
  private final long mLastAccessTimeMs;
  private final String mOwner;
  private final String mGroup;
  private final int mMode;

  private FuseURIStatus(String name, long length, boolean completed, boolean folder,
      long lastModificationTimeMs, long lastAccessTimeMs, String owner, String group, int mode) {
    mName = name;
    mLength = length;
    mCompleted = completed;
    mFolder = folder;
    mLastModificationTimeMs = lastModificationTimeMs;
    mLastAccessTimeMs = lastAccessTimeMs;
    mOwner = owner;
    mGroup = group;
    mMode = mode;
  }

  /**
   * @return the file name
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the file length
   */
  public long getLength() {
    return mLength;
  }

  /**
   * @return the file last modification time (in milliseconds)
   */
  public long getLastModificationTimeMs() {
    return mLastModificationTimeMs;
  }

  /**
   * @return the file last access time (in milliseconds)
   */
  public long getLastAccessTimeMs() {
    return mLastAccessTimeMs;
  }

  /**
   * @return the file owner
   */
  public String getOwner() {
    return mOwner;
  }

  /**
   * @return the file owner group
   */
  public String getGroup() {
    return mGroup;
  }

  /**
   * @return the file mode bits
   */
  public int getMode() {
    return mMode;
  }

  /**
   * @return whether the file is completed
   */
  public boolean isCompleted() {
    return mCompleted;
  }

  /**
   * @return whether the file is a folder
   */
  public boolean isFolder() {
    return mFolder;
  }

  /**
   * Transforms from {@link URIStatus} to {@link FuseURIStatus}.
   *
   * @param status the URI status
   * @return a transformed {@link FuseURIStatus}
   */
  public static FuseURIStatus create(URIStatus status) {
    return new FuseURIStatus.Builder().setName(status.getName())
        .setLength(status.getLength()).setCompleted(status.isCompleted())
        .setFolder(status.isFolder()).setLastModificationTimeMs(status.getLastModificationTimeMs())
        .setLastAccessTimeMs(status.getLastAccessTimeMs()).setOwner(status.getOwner())
        .setGroup(status.getGroup()).setMode(status.getMode()).build();
  }

  /**
   * @return a new Builder for {@link FuseURIStatus}
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for {@link FuseURIStatus}.
   */
  public static final class Builder {
    private String mName = "";
    private long mLength;
    private boolean mCompleted;
    private boolean mFolder;
    private long mLastModificationTimeMs;
    private long mLastAccessTimeMs;
    private String mOwner = "";
    private String mGroup = "";
    private int mMode;

    /**
     * Creates a builder.
     */
    public Builder() {}

    /**
     * Sets the name.
     *
     * @param name name to set
     * @return the builder
     */
    public Builder setName(String name) {
      mName = name;
      return this;
    }

    /**
     * Sets the length.
     *
     * @param length length to set
     * @return the builder
     */
    public Builder setLength(long length) {
      mLength = length;
      return this;
    }

    /**
     * Sets whether the URI is completed.
     *
     * @param completed whether it's completed
     * @return the builder
     */
    public Builder setCompleted(boolean completed) {
      mCompleted = completed;
      return this;
    }

    /**
     * Sets whether the URI represents a folder.
     *
     * @param folder whether it's a folder
     * @return the builder
     */
    public Builder setFolder(boolean folder) {
      mFolder = folder;
      return this;
    }

    /**
     * Sets the last modification time in milliseconds.
     *
     * @param lastModificationTimeMs last modification time in milliseconds
     * @return the builder
     */
    public Builder setLastModificationTimeMs(long lastModificationTimeMs) {
      mLastModificationTimeMs = lastModificationTimeMs;
      return this;
    }

    /**
     * Sets the last access time in milliseconds.
     *
     * @param lastAccessTimeMs last access time in milliseconds
     * @return the builder
     */
    public Builder setLastAccessTimeMs(long lastAccessTimeMs) {
      mLastAccessTimeMs = lastAccessTimeMs;
      return this;
    }

    /**
     * Sets the owner.
     *
     * @param owner the owner
     * @return the builder
     */
    public Builder setOwner(String owner) {
      mOwner = owner;
      return this;
    }

    /**
     * Sets the group.
     *
     * @param group the group
     * @return the builder
     */
    public Builder setGroup(String group) {
      mGroup = group;
      return this;
    }

    /**
     * Sets the mode.
     *
     * @param mode mode to set
     * @return the builder
     */
    public Builder setMode(int mode) {
      mMode = mode;
      return this;
    }

    /**
     * @return a new {@link FuseURIStatus}
     */
    public FuseURIStatus build() {
      return new FuseURIStatus(mName, mLength, mCompleted, mFolder,
          mLastModificationTimeMs, mLastAccessTimeMs, mOwner, mGroup, mMode);
    }
  }
}
