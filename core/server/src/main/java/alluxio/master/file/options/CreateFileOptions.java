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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.master.MasterContext;
import alluxio.thrift.CreateFileTOptions;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a file.
 */
@NotThreadSafe
public final class CreateFileOptions {
  private long mBlockSizeBytes;
  private long mOperationTimeMs;
  private boolean mPersisted;
  private boolean mRecursive;
  private long mTtl;

  /**
   * @return the default {@link CreateFileOptions}
   */
  public static CreateFileOptions defaults() {
    return new CreateFileOptions();
  }

  /**
   * Creates a new instance of {@link CreateFileOptions} from {@link CreateFileTOptions}.
   *
   * @param options Thrift options
   */
  public CreateFileOptions(CreateFileTOptions options) {
    mBlockSizeBytes = options.getBlockSizeBytes();
    mOperationTimeMs = System.currentTimeMillis();
    mPersisted = options.isPersisted();
    mRecursive = options.isRecursive();
    mTtl = options.getTtl();
  }

  private CreateFileOptions() {
    Configuration conf = MasterContext.getConf();
    mBlockSizeBytes = conf.getBytes(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
    mOperationTimeMs = System.currentTimeMillis();
    mPersisted = false;
    mRecursive = false;
    mTtl = Constants.NO_TTL;
  }

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @return the operation time
   */
  public long getOperationTimeMs() {
    return mOperationTimeMs;
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
   * @return the TTL (time to live) value; it identifies duration (in milliseconds) the created file
   *         should be kept around before it is automatically deleted
   */
  public long getTtl() {
    return mTtl;
  }

  /**
   * @param blockSizeBytes the block size to use
   * @return the updated options object
   */
  public CreateFileOptions setBlockSizeBytes(long blockSizeBytes) {
    mBlockSizeBytes = blockSizeBytes;
    return this;
  }

  /**
   * @param operationTimeMs the operation time to use
   * @return the updated options object
   */
  public CreateFileOptions setOperationTimeMs(long operationTimeMs) {
    mOperationTimeMs = operationTimeMs;
    return this;
  }

  /**
   * @param persisted the persisted flag to use; it specifies whether the object to created is
   *        persisted in UFS
   * @return the updated options object
   */
  public CreateFileOptions setPersisted(boolean persisted) {
    mPersisted = persisted;
    return this;
  }

  /**
   * @param recursive the recursive flag value to use; it specifies whether parent directories
   *        should be created if they do not already exist
   * @return the updated options object
   */
  public CreateFileOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  /**
   * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted
   * @return the updated options object
   */
  public CreateFileOptions setTtl(long ttl) {
    mTtl = ttl;
    return this;
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("blockSizeBytes", mBlockSizeBytes)
        .add("operationTimeMs", mOperationTimeMs).add("persisted", mPersisted)
        .add("recursive", mRecursive).add("ttl", mTtl).toString();
  }
}
