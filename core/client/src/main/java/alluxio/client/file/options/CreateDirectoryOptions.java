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

package alluxio.client.file.options;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.client.WriteType;
import alluxio.security.authorization.Mode;
import alluxio.thrift.CreateDirectoryTOptions;
import alluxio.wire.ThriftUtils;
import alluxio.wire.TtlAction;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a directory.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class CreateDirectoryOptions {
  private boolean mAllowExists;
  private Mode mMode;
  private long mTtl;
  private TtlAction mTtlAction;
  private boolean mRecursive;
  private WriteType mWriteType;

  /**
   * @return the default {@link CreateDirectoryOptions}
   */
  public static CreateDirectoryOptions defaults() {
    return new CreateDirectoryOptions();
  }

  private CreateDirectoryOptions() {
    mRecursive = false;
    mAllowExists = false;
    mMode = Mode.defaults().applyDirectoryUMask();
    mTtl = Constants.NO_TTL;
    mTtlAction = TtlAction.DELETE;
    mWriteType =
        Configuration.getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class);
  }

  /**
   * @return the mode of the directory to create
   */
  public Mode getMode() {
    return mMode;
  }

  /**
   * @return the write type
   */
  public WriteType getWriteType() {
    return mWriteType;
  }

  /**
   * @return the allowExists flag value; it specifies whether an exception should be thrown if the
   *         directory being made already exists
   */
  public boolean isAllowExists() {
    return mAllowExists;
  }

  /**
   * @return the TTL (time to live) value; it identifies duration (in milliseconds)
   *         the created directory should be kept around before it is automatically deleted
   */
  public long getTtl() {
    return mTtl;
  }

  /**
   * @return the {@link TtlAction}
   */
  public TtlAction getTtlAction() {
    return mTtlAction;
  }

  /**
   * @return the recursive flag value; it specifies whether parent directories should be created if
   *         they do not already exist
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @param allowExists the allowExists flag value to use; it specifies whether an exception
   *        should be thrown if the directory being made already exists.
   * @return the updated options object
   */
  public CreateDirectoryOptions setAllowExists(boolean allowExists) {
    mAllowExists = allowExists;
    return this;
  }

  /**
   * @param mode the mode to be set
   * @return the updated options object
   */
  public CreateDirectoryOptions setMode(Mode mode) {
    mMode = mode;
    return this;
  }

  /**
   * @param recursive the recursive flag value to use; it specifies whether parent directories
   *        should be created if they do not already exist
   * @return the updated options object
   */
  public CreateDirectoryOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  /**
   * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created directory should be kept around before it is automatically deleted,
   *        no matter whether the file is pinned
   * @return the updated options object
   */
  public CreateDirectoryOptions setTtl(long ttl) {
    mTtl = ttl;
    return this;
  }

  /**
   * @param ttlAction the {@link TtlAction} to use
   * @return the updated options object
   */
  public CreateDirectoryOptions setTtlAction(TtlAction ttlAction) {
    mTtlAction = ttlAction;
    return this;
  }

  /**
   * @param writeType the write type to use
   * @return the updated options object
   */
  public CreateDirectoryOptions setWriteType(WriteType writeType) {
    mWriteType = writeType;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateDirectoryOptions)) {
      return false;
    }
    CreateDirectoryOptions that = (CreateDirectoryOptions) o;
    return Objects.equal(mAllowExists, that.mAllowExists)
        && Objects.equal(mMode, that.mMode)
        && Objects.equal(mRecursive, that.mRecursive)
        && Objects.equal(mTtl, that.mTtl)
        && Objects.equal(mTtlAction, that.mTtlAction)
        && Objects.equal(mWriteType, that.mWriteType);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mAllowExists, mMode, mRecursive, mTtl,
        mTtlAction, mWriteType);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("allowExists", mAllowExists)
        .add("mode", mMode)
        .add("recursive", mRecursive)
        .add("ttl", mTtl)
        .add("ttlAction", mTtlAction)
        .add("writeType", mWriteType)
        .toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public CreateDirectoryTOptions toThrift() {
    CreateDirectoryTOptions options = new CreateDirectoryTOptions();
    options.setAllowExists(mAllowExists);
    options.setRecursive(mRecursive);
    options.setTtl(mTtl);
    options.setTtlAction(ThriftUtils.toThrift(mTtlAction));
    options.setPersisted(mWriteType.isThrough());
    if (mMode != null) {
      options.setMode(mMode.toShort());
    }
    return options;
  }
}
