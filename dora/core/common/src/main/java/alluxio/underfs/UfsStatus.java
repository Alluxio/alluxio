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

package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.util.CommonUtils;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Information about a file or a directory in the under file system. Listing contents in a
 * {@link UnderFileSystem} returns entries of this class.
 */
@NotThreadSafe
public abstract class UfsStatus {
  protected final boolean mIsDirectory;
  /** Last modified epoch time in ms, or null if it is not available. */
  protected final Long mLastModifiedTimeMs;
  protected String mName;

  /**
   * @return the full ufs path
   */
  public AlluxioURI getUfsFullPath() {
    return mUfsFullPath;
  }

  /**
   * @param ufsFullPath the ufs full path
   */
  public void setUfsFullPath(AlluxioURI ufsFullPath) {
    mUfsFullPath = ufsFullPath;
  }

  protected AlluxioURI mUfsFullPath;

  // Permissions
  protected final String mOwner;
  protected final String mGroup;
  protected final short mMode;

  protected final Map<String, byte[]> mXAttr;

  /**
   * Creates new instance of {@link UfsStatus}.
   *
   * @param name relative path of file or directory
   * @param isDirectory whether the path is a directory
   * @param owner of the file
   * @param group of the file
   * @param mode of the file
   * @param lastModifiedTimeMs last modified epoch time in ms, or null if it is not available
   * @param xAttrs any extended attributes on the inode
   */
  protected UfsStatus(String name, boolean isDirectory, String owner, String group, short mode,
      @Nullable Long lastModifiedTimeMs, @Nullable Map<String, byte[]> xAttrs) {
    mIsDirectory = isDirectory;
    mName = name;
    mOwner = owner;
    mGroup = group;
    mMode = mode;
    mLastModifiedTimeMs = lastModifiedTimeMs;
    mXAttr = xAttrs;
  }

  /**
   * Creates a new instance of {@link UfsStatus} as a copy.
   *
   * @param status file information to copy
   */
  protected UfsStatus(UfsStatus status) {
    mIsDirectory = status.mIsDirectory;
    mName = status.mName;
    mOwner = status.mOwner;
    mGroup = status.mGroup;
    mMode = status.mMode;
    mLastModifiedTimeMs = status.mLastModifiedTimeMs;
    mXAttr = status.mXAttr == null ? null : new HashMap<>(status.mXAttr);
  }

  /**
   * Create a copy of {@link UfsStatus}.
   *
   * @return new instance as a copy
   */
  public abstract UfsStatus copy();

  /**
   * Converts an array of UFS file status to a listing result where each element in the array is
   * a file or directory name.
   *
   * @param children array of listing statuses
   * @return array of file or directory names, or null if the input is null
   */
  @Nullable
  public static String[] convertToNames(UfsStatus[] children) {
    if (children == null) {
      return null;
    }
    String[] ret = new String[children.length];
    for (int i = 0; i < children.length; ++i) {
      ret[i] = children[i].getName();
    }
    return ret;
  }

  /**
   * @return true, if the path is a directory
   */
  public boolean isDirectory() {
    return mIsDirectory;
  }

  /**
   * @return true, if the path is a file
   */
  public boolean isFile() {
    return !mIsDirectory;
  }

  /**
   * Gets the group of the given path.
   *
   * @return the group of the file
   */
  public String getGroup() {
    return mGroup;
  }

  /**
   * Gets the UTC time of when the indicated path was modified recently in ms, or null if the last
   * modified time is not available.
   *
   * @return modification time in milliseconds
   */
  @Nullable
  public Long getLastModifiedTime() {
    return mLastModifiedTimeMs;
  }

  /**
   * Gets the mode of the given path in short format, e.g 0700.
   *
   * @return the mode of the file
   */
  public short getMode() {
    return mMode;
  }

  /**
   * @return name of file or directory
   */
  public String getName() {
    return mName;
  }

  /**
   * Gets the owner of the given path.
   *
   * @return the owner of the path
   */
  public String getOwner() {
    return mOwner;
  }

  /**
   * Returns the extended attributes from the Ufs, if any.
   * @return a map of the extended attributes. If none, the map will be empty
   */
  @Nullable
  public Map<String, byte[]> getXAttr() {
    return mXAttr;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mName, mIsDirectory, mOwner, mGroup, mMode, mXAttr);
  }

  /**
   * Set the name of file or directory.
   *
   * @param name of entry
   * @return this object
   */
  public UfsStatus setName(String name) {
    mName = name;
    return this;
  }

  protected MoreObjects.ToStringHelper toStringHelper() {
    return MoreObjects.toStringHelper(this)
        .add("isDirectory", mIsDirectory)
        .add("lastModifiedTimeMs", mLastModifiedTimeMs)
        .add("name", mName)
        .add("owner", mOwner)
        .add("group", mGroup)
        .add("mode", mMode)
        .add("xAttr", mXAttr)
        .add("ufsFullPath", mUfsFullPath);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UfsStatus)) {
      return false;
    }
    UfsStatus that = (UfsStatus) o;
    return Objects.equal(mName, that.mName)
        && Objects.equal(mIsDirectory, that.mIsDirectory)
        && Objects.equal(mOwner, that.mOwner)
        && Objects.equal(mGroup, that.mGroup)
        && Objects.equal(mMode, that.mMode)
        && Objects.equal(mXAttr, that.mXAttr)
        && Objects.equal(mUfsFullPath, that.mUfsFullPath);
  }

  /**
   * @return the UfsStatus protobuf
   */
  public alluxio.grpc.UfsStatus toProto() {
    alluxio.grpc.UfsStatus.Builder builder = alluxio.grpc.UfsStatus.newBuilder();
    builder.setName(getName());
    builder.setIsDirectory(isDirectory());
    if (mLastModifiedTimeMs != null) {
      builder.setLastModifiedTimeMs(mLastModifiedTimeMs);
    }
    builder.setOwner(mOwner);
    builder.setGroup(mGroup);
    builder.setMode(mMode);
    if (mXAttr != null) {
      builder.putAllXattr(CommonUtils.convertToByteString(mXAttr));
    }
    if (mUfsFullPath != null) {
      builder.setUfsFullPath(mUfsFullPath.toString());
    }
    if (this instanceof UfsFileStatus) {
      builder.setUfsFileStatus(
          alluxio.grpc.UfsFileStatus.newBuilder()
              .setBlockSize(((UfsFileStatus) this).getBlockSize())
              .setContentHash(((UfsFileStatus) this).getContentHash())
              .setContentLength(((UfsFileStatus) this).getContentLength())
              .build()
      );
    }
    return builder.build();
  }

  /**
   * @param ufsStatus the UfsStatus protobuf
   * @return the UfsStatus model
   */
  public static UfsStatus fromProto(alluxio.grpc.UfsStatus ufsStatus) {
    final UfsStatus status;
    if (ufsStatus.getIsDirectory()) {
      status = new UfsDirectoryStatus(
          ufsStatus.getName(), ufsStatus.getOwner(), ufsStatus.getGroup(),
          (short) ufsStatus.getMode(),
          ufsStatus.hasLastModifiedTimeMs() ? ufsStatus.getLastModifiedTimeMs() : null,
          CommonUtils.convertFromByteString(ufsStatus.getXattrMap())
      );
    } else {
      status = new UfsFileStatus(
          ufsStatus.getName(), ufsStatus.getUfsFileStatus().getContentHash(),
          ufsStatus.getUfsFileStatus().getContentLength(),
          ufsStatus.hasLastModifiedTimeMs() ? ufsStatus.getLastModifiedTimeMs() : null,
          ufsStatus.getOwner(), ufsStatus.getGroup(), (short) ufsStatus.getMode(),
          CommonUtils.convertFromByteString(ufsStatus.getXattrMap()),
          ufsStatus.getUfsFileStatus().getBlockSize()
      );
    }
    if (ufsStatus.hasUfsFullPath()) {
      status.setUfsFullPath(new AlluxioURI(ufsStatus.getUfsFullPath()));
    }
    return status;
  }

  /**
   * @return the object converted to UfsFileStatus type
   */
  public UfsFileStatus asUfsFileStatus() {
    return (UfsFileStatus) this;
  }
}
