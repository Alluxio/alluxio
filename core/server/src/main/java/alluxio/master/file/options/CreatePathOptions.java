/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.options;

import alluxio.Constants;
import alluxio.master.MasterContext;
import alluxio.security.authorization.PermissionStatus;
import alluxio.thrift.CreateDirectoryTOptions;
import alluxio.thrift.CreateFileTOptions;

import com.google.common.base.Objects;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method option for creating a path.
 */
@NotThreadSafe
public final class CreatePathOptions {
  private boolean mAllowExists;
  private long mBlockSizeBytes;
  private boolean mDirectory;
  private boolean mMountPoint;
  private long mOperationTimeMs;
  private PermissionStatus mPermissionStatus;
  private boolean mPersisted;
  private boolean mRecursive;
  private long mTtl;

  /**
   * @return the default {@link CreatePathOptions}
   * @throws IOException if I/O error occurs
   */
  public static CreatePathOptions defaults() throws IOException {
    return new CreatePathOptions();
  }

  /**
   * Creates a new instance of {@link CreatePathOptions} from {@link CreateDirectoryTOptions}.
   *
   * @param options the {@link CreateDirectoryTOptions} to use
   * @throws IOException if an I/O error occurs
   */
  public CreatePathOptions(CreateDirectoryTOptions options) throws IOException {
    this();
    mAllowExists = options.isAllowExists();
    mDirectory = true;
    mPersisted = options.isPersisted();
    mRecursive = options.isRecursive();
  }

  /**
   * Creates a new instance of {@link CreatePathOptions} from {@link CreateFileTOptions}.
   *
   * @param options the {@link CreateFileTOptions} to use
   * @throws IOException if an I/O error occurs
   */
  public CreatePathOptions(CreateFileTOptions options) throws IOException {
    this();
    mBlockSizeBytes = options.getBlockSizeBytes();
    mDirectory = false;
    mPersisted = options.isPersisted();
    mRecursive = options.isRecursive();
    mTtl = options.getTtl();
  }

  private CreatePathOptions() throws IOException {
    mAllowExists = false;
    mBlockSizeBytes = MasterContext.getConf().getBytes(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
    mDirectory = false;
    mMountPoint = false;
    mOperationTimeMs = System.currentTimeMillis();
    mPermissionStatus = PermissionStatus.get(MasterContext.getConf(), true);
    mPersisted = false;
    mRecursive = false;
    mTtl = Constants.NO_TTL;
  }

  /**
   * @return the allowExists flag; it specifies whether an exception should be thrown if the object
   *         being made already exists
   */
  public boolean isAllowExists() {
    return mAllowExists;
  }

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @return the directory flag; it specifies whether the object to create is a directory
   */
  public boolean isDirectory() {
    return mDirectory;
  }

  /**
   * @return the operation time
   */
  public long getOperationTimeMs() {
    return mOperationTimeMs;
  }

  /**
   * @return the mount point flag; it specifies whether the object to create is a mount point
   */
  public boolean isMountPoint() {
    return mMountPoint;
  }

  /**
   * @return the permission status
   */
  public PermissionStatus getPermissionStatus() {
    return mPermissionStatus;
  }

  /**
   * @return the persisted flag; it specifies whether the object to create is persisted in UFS
   */
  public boolean isPersisted() {
    return mPersisted;
  }

  /**
   * @return the recursive flag value; it specifies whether parent directories should be created if
   *         they do not already exist
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @return the TTL (time to live) value; it identifies duration (in seconds) the created file
   *         should be kept around before it is automatically deleted
   */
  public long getTtl() {
    return mTtl;
  }

  /**
   * @param allowExists the allowExists flag value to use; it specifies whether an exception
   *        should be thrown if the object being made already exists.
   * @return the updated options object
   */
  public CreatePathOptions setAllowExists(boolean allowExists) {
    mAllowExists = allowExists;
    return this;
  }

  /**
   * @param blockSizeBytes the block size to use
   * @return the updated options object
   */
  public CreatePathOptions setBlockSizeBytes(long blockSizeBytes) {
    mBlockSizeBytes = blockSizeBytes;
    return this;
  }

  /**
   * @param directory the directory flag to use; it specifies whether the object to create is a
   *        directory
   * @return the updated options object
   */
  public CreatePathOptions setDirectory(boolean directory) {
    mDirectory = directory;
    return this;
  }

  /**
   * @param mountPoint the mount point flag to use; it specifies whether the object to create is
   *        a mount point
   * @return the updated options object
   */
  public CreatePathOptions setMountPoint(boolean mountPoint) {
    mMountPoint = mountPoint;
    return this;
  }

  /**
   * @param operationTimeMs the operation time to use
   * @return the updated options object
   */
  public CreatePathOptions setOperationTimeMs(long operationTimeMs) {
    mOperationTimeMs = operationTimeMs;
    return this;
  }

  /**
   * @param permissionStatus the permission status to use
   * @return the updated options object
   */
  public CreatePathOptions setPermissionStatus(PermissionStatus permissionStatus) {
    mPermissionStatus = permissionStatus;
    return this;
  }

  /**
   * @param persisted the persisted flag to use; it specifies whether the object to create is
   *        persisted in UFS
   * @return the updated options object
   */
  public CreatePathOptions setPersisted(boolean persisted) {
    mPersisted = persisted;
    return this;
  }

  /**
   * @param recursive the recursive flag value to use; it specifies whether parent directories
   *        should be created if they do not already exist
   * @return the updated options object
   */
  public CreatePathOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  /**
   * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted
   * @return the updated options object
   */
  public CreatePathOptions setTtl(long ttl) {
    mTtl = ttl;
    return this;
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("allowExists", mAllowExists)
        .add("blockSizeBytes", mBlockSizeBytes).add("directory", mDirectory)
        .add("mountPoint", mMountPoint).add("operationTimeMs", mOperationTimeMs)
        .add("persisted", mPersisted).add("recursive", mRecursive).add("ttl", mTtl)
        .add("permissionStatus", mPermissionStatus).toString();
  }
}
