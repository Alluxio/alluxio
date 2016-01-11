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

package tachyon.client.file.options;

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.annotation.PublicApi;
import tachyon.client.ClientContext;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.WriteType;
import tachyon.client.file.policy.FileWriteLocationPolicy;
import tachyon.thrift.CreateTOptions;
import tachyon.util.CommonUtils;

@PublicApi
public final class CreateFileOptions {
  private boolean mRecursive;
  private long mBlockSizeBytes;
  private FileWriteLocationPolicy mLocationPolicy;
  private TachyonStorageType mTachyonStorageType;
  private long mTTL;
  private UnderStorageType mUnderStorageType;

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
    mRecursive = true;
    mBlockSizeBytes = ClientContext.getConf().getBytes(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
    try {
      mLocationPolicy =
          CommonUtils
              .createNewClassInstance(
                  ClientContext.getConf().<FileWriteLocationPolicy>getClass(
                      Constants.USER_FILE_WRITE_LOCATION_POLICY),
                  new Class[]{}, new Object[]{});
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    WriteType defaultWriteType =
        ClientContext.getConf().getEnum(Constants.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class);
    mTachyonStorageType = defaultWriteType.getTachyonStorageType();
    mUnderStorageType = defaultWriteType.getUnderStorageType();
    mTTL = Constants.NO_TTL;
  }

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @return the hostname
   */
  public FileWriteLocationPolicy getLocationPolicy() {
    return mLocationPolicy;
  }

  /**
   * @return the Tachyon storage type
   */
  public TachyonStorageType getTachyonStorageType() {
    return mTachyonStorageType;
  }

  /**
   * @return the TTL (time to live) value; it identifies duration (in milliseconds) the created file
   *         should be kept around before it is automatically deleted
   */
  public long getTTL() {
    return mTTL;
  }

  /**
   * @return the under storage type
   */
  public UnderStorageType getUnderStorageType() {
    return mUnderStorageType;
  }

  /**
   * @return whether or not the recursive flag is set
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @param blockSizeBytes the block size to use
   * @return the builder
   */
  public CreateFileOptions setBlockSizeBytes(long blockSizeBytes) {
    mBlockSizeBytes = blockSizeBytes;
    return this;
  }

  /**
   * @param locationPolicy the location policy to use
   * @return the builder
   */
  public CreateFileOptions setLocationPolicy(FileWriteLocationPolicy locationPolicy) {
    mLocationPolicy = locationPolicy;
    return this;
  }

  /**
   * @param recursive whether or not to recursively create the file's parents
   * @return the updated object
   */
  public CreateFileOptions setRecurisve(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  /**
   * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted, no matter whether
   *        the file is pinned
   * @return this object
   */
  public CreateFileOptions setTTL(long ttl) {
    mTTL = ttl;
    return this;
  }

  /**
   * @param writeType the {@link tachyon.client.WriteType} to use for this operation. This will
   *        override both the TachyonStorageType and UnderStorageType.
   * @return this object
   */
  public CreateFileOptions setWriteType(WriteType writeType) {
    mTachyonStorageType = writeType.getTachyonStorageType();
    mUnderStorageType = writeType.getUnderStorageType();
    return this;
  }

  /**
   * @return representation of this object in the form of {@link OutStreamOptions}
   */
  public OutStreamOptions toOutStreamOptions() {
    OutStreamOptions.Builder builder = new OutStreamOptions.Builder();
    builder.setBlockSizeBytes(mBlockSizeBytes).setLocationPolicy(mLocationPolicy)
        .setTachyonStorageType(mTachyonStorageType).setTTL(mTTL)
        .setUnderStorageType(mUnderStorageType);
    return builder.build();
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("CreateFileOptions(");
    sb.append(super.toString()).append(", BlockSizeBytes: ").append(mBlockSizeBytes);
    sb.append(", Location Policy: ").append(mLocationPolicy);
    sb.append(", TachyonStorageType: ").append(mTachyonStorageType.toString());
    sb.append(", UnderStorageType: ").append(mUnderStorageType.toString());
    sb.append(", TTL: ").append(mTTL);
    sb.append(")");
    return sb.toString();
  }

  /**
   * @return Thrift representation of the options
   */
  // TODO(calvin): Rename CreateTOptions to CreateFileTOptions
  public CreateTOptions toThrift() {
    CreateTOptions options = new CreateTOptions();
    options.setBlockSizeBytes(mBlockSizeBytes);
    options.setPersisted(mUnderStorageType.isSyncPersist());
    options.setRecursive(mRecursive);
    options.setTtl(mTTL);
    return options;
  }
}
