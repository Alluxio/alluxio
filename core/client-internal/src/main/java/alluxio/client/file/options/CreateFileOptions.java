/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.client.file.options;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Throwables;

import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.client.ClientContext;
import alluxio.client.AlluxioStorageType;
import alluxio.client.UnderStorageType;
import alluxio.client.WriteType;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.Configuration;
import alluxio.thrift.CreateFileTOptions;
import alluxio.util.CommonUtils;

/**
 * Method options for creating a file.
 */
@PublicApi
@NotThreadSafe
public final class CreateFileOptions {
  private boolean mRecursive;
  private long mBlockSizeBytes;
  private FileWriteLocationPolicy mLocationPolicy;
  private long mTtl;
  private WriteType mWriteType;

  /**
   * @return the default {@link OutStreamOptions}
   */
  public static CreateFileOptions defaults() {
    return new CreateFileOptions();
  }

  /**
   * Creates a new instance with defaults from the configuration.
   */
  private CreateFileOptions() {
    Configuration conf = ClientContext.getConf();
    mRecursive = true;
    mBlockSizeBytes = conf.getBytes(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
    try {
      mLocationPolicy =
          CommonUtils.createNewClassInstance(
              conf.<FileWriteLocationPolicy>getClass(Constants.USER_FILE_WRITE_LOCATION_POLICY),
              new Class[]{}, new Object[]{});
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    mWriteType = conf.getEnum(Constants.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class);
    mTtl = Constants.NO_TTL;
  }

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @return the location policy for writes to Alluxio storage
   */
  public FileWriteLocationPolicy getLocationPolicy() {
    return mLocationPolicy;
  }

  /**
   * @return the Alluxio storage type
   */
  public AlluxioStorageType getAlluxioStorageType() {
    return mWriteType.getAlluxioStorageType();
  }

  /**
   * @return the TTL (time to live) value; it identifies duration (in milliseconds) the created file
   *         should be kept around before it is automatically deleted
   */
  public long getTtl() {
    return mTtl;
  }

  /**
   * @return the under storage type
   */
  public UnderStorageType getUnderStorageType() {
    return mWriteType.getUnderStorageType();
  }

  /**
   * @return whether or not the recursive flag is set
   */
  public boolean isRecursive() {
    return mRecursive;
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
   * @param locationPolicy the location policy to use
   * @return the updated options object
   */
  public CreateFileOptions setLocationPolicy(FileWriteLocationPolicy locationPolicy) {
    mLocationPolicy = locationPolicy;
    return this;
  }

  /**
   * @param recursive whether or not to recursively create the file's parents
   * @return the updated options object
   */
  public CreateFileOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  /**
   * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted, no matter whether
   *        the file is pinned
   * @return the updated options object
   */
  public CreateFileOptions setTtl(long ttl) {
    mTtl = ttl;
    return this;
  }

  /**
   * @param writeType the {@link WriteType} to use for this operation. This will override both the
   *        {@link AlluxioStorageType} and {@link UnderStorageType}.
   * @return the updated options object
   */
  public CreateFileOptions setWriteType(WriteType writeType) {
    mWriteType = writeType;
    return this;
  }

  /**
   * @return representation of this object in the form of {@link OutStreamOptions}
   */
  public OutStreamOptions toOutStreamOptions() {
    return OutStreamOptions.defaults().setBlockSizeBytes(mBlockSizeBytes)
        .setLocationPolicy(mLocationPolicy).setTtl(mTtl).setWriteType(mWriteType);
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("CreateFileOptions(");
    sb.append(super.toString()).append(", BlockSizeBytes: ").append(mBlockSizeBytes);
    sb.append(", TTL: ").append(mTtl);
    sb.append(", Location Policy: ").append(mLocationPolicy);
    sb.append(", WriteType: ").append(mWriteType.toString());
    sb.append(")");
    return sb.toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public CreateFileTOptions toThrift() {
    CreateFileTOptions options = new CreateFileTOptions();
    options.setBlockSizeBytes(mBlockSizeBytes);
    options.setPersisted(mWriteType.getUnderStorageType().isSyncPersist());
    options.setRecursive(mRecursive);
    options.setTtl(mTtl);
    return options;
  }
}
