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
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;

/**
 * Method option for writing a file.
 */
@PublicApi
public final class OutStreamOptions {
  private long mBlockSizeBytes;
  private long mTTL;
  private FileWriteLocationPolicy mLocationPolicy;
  private WriteType mWriteType;

  /**
   * @return the default {@link OutStreamOptions}
   */
  public static OutStreamOptions defaults() {
    return new OutStreamOptions();
  }

  private OutStreamOptions() {
    TachyonConf conf = ClientContext.getConf();
    mBlockSizeBytes = conf.getBytes(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
    mTTL = Constants.NO_TTL;
    try {
      mLocationPolicy =
          CommonUtils.createNewClassInstance(ClientContext.getConf()
              .<FileWriteLocationPolicy>getClass(Constants.USER_FILE_WRITE_LOCATION_POLICY),
              new Class[] {}, new Object[] {});
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    mWriteType = conf.getEnum(Constants.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class);
  }

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @return the location policy
   */
  public FileWriteLocationPolicy getLocationPolicy() {
    return mLocationPolicy;
  }

  /**
   * @return the Tachyon storage type
   */
  public TachyonStorageType getTachyonStorageType() {
    return mWriteType.getTachyonStorageType();
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
    return mWriteType.getUnderStorageType();
  }

  /**
   * Sets the size of the block in bytes.
   *
   * @param blockSizeBytes the block size to use
   * @return the updated options object
   */
  public OutStreamOptions setBlockSizeBytes(long blockSizeBytes) {
    mBlockSizeBytes = blockSizeBytes;
    return this;
  }

  /**
   * Sets the time to live.
   *
   * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted, no matter
   *        whether the file is pinned
   * @return the updated options object
   */
  public OutStreamOptions setTTL(long ttl) {
    mTTL = ttl;
    return this;
  }

  /**
   * @param locationPolicy the location policy for file write
   * @return the updated options object
   */
  public OutStreamOptions setLocationPolicy(FileWriteLocationPolicy locationPolicy) {
    mLocationPolicy = locationPolicy;
    return this;
  }

  /**
   * Sets the {@link WriteType}.
   *
   * @param writeType the {@link tachyon.client.WriteType} to use for this operation. This will
   *                  override both the TachyonStorageType and UnderStorageType.
   * @return the updated options object
   */
  public OutStreamOptions setWriteType(WriteType writeType) {
    mWriteType = writeType;
    return this;
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("OutStreamOptions(");
    sb.append(super.toString()).append(", BlockSizeBytes: ").append(mBlockSizeBytes);
    sb.append(", TTL: ").append(mTTL);
    sb.append(", LocationPolicy: ").append(mLocationPolicy.toString());
    sb.append(", WriteType: ").append(mWriteType.toString());
    sb.append(")");
    return sb.toString();
  }
}
