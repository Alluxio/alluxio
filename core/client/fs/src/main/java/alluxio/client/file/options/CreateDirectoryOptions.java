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
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.client.WriteType;
import alluxio.security.authorization.Mode;
import alluxio.util.ModeUtils;
import alluxio.wire.CommonOptions;
import alluxio.wire.TtlAction;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;

import java.util.Collections;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a directory.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class CreateDirectoryOptions extends alluxio.file.options.CreateDirectoryOptions {
  private WriteType mWriteType;

  /**
   * @return the default {@link CreateDirectoryOptions}
   */
  public static CreateDirectoryOptions defaults() {
    return new CreateDirectoryOptions();
  }

  private CreateDirectoryOptions() {
    // TODO(adit): redundant definition in CreateFileOptions
    mCommonOptions = CommonOptions.defaults()
        .setTtl(Configuration.getLong(PropertyKey.USER_FILE_CREATE_TTL)).setTtlAction(
            Configuration.getEnum(PropertyKey.USER_FILE_CREATE_TTL_ACTION, TtlAction.class));
    mMode = ModeUtils.applyFileUMask(Mode.defaults());
    mAcl = Collections.emptyList();
    mRecursive = true;

    mWriteType = Configuration.getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class);
    // TODO(adit):
    mPersisted = mWriteType.isThrough();
  }

  /**
   * @return the write type
   */
  public WriteType getWriteType() {
    return mWriteType;
  }

  /**
   * @param writeType the write type to use
   * @return the updated options object
   */
  public CreateDirectoryOptions setWriteType(WriteType writeType) {
    mWriteType = writeType;
    // TODO(adit):
    mPersisted = mWriteType.isThrough();
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
        && Objects.equal(mCommonOptions, that.mCommonOptions)
        && Objects.equal(mMode, that.mMode)
        && Objects.equal(mRecursive, that.mRecursive)
        && Objects.equal(mWriteType, that.mWriteType);
  }

  @Override
  public int hashCode() {
    return Objects
        .hashCode(mAllowExists, mCommonOptions, mMode, mRecursive, mWriteType);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("commonOptions", mCommonOptions)
        .add("allowExists", mAllowExists)
        .add("mode", mMode)
        .add("recursive", mRecursive)
        .add("writeType", mWriteType)
        .toString();
  }
}
