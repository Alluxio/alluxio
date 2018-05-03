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

package alluxio.master.file.options;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.security.authorization.Mode;
import alluxio.thrift.CreateFileTOptions;
import alluxio.util.SecurityUtils;
import alluxio.wire.CommonOptions;
import alluxio.wire.TtlAction;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a file.
 */
@NotThreadSafe
public final class CreateFileOptions extends CreatePathOptions<CreateFileOptions> {
  private long mBlockSizeBytes;
  private boolean mCacheable;

  /**
   * @return the default {@link CreateFileOptions}
   */
  public static CreateFileOptions defaults() {
    return new CreateFileOptions();
  }

  /**
   * Constructs an instance of {@link CreateFileOptions} from {@link CreateFileTOptions}. The option
   * of permission is constructed with the username obtained from thrift transport.
   *
   * @param options the {@link CreateFileTOptions} to use
   */
  public CreateFileOptions(CreateFileTOptions options) {
    this();
    if (options != null) {
      if (options.isSetCommonOptions()) {
        mCommonOptions = new CommonOptions(options.getCommonOptions());
      }
      mBlockSizeBytes = options.getBlockSizeBytes();
      mPersisted = options.isPersisted();
      mRecursive = options.isRecursive();
      mTtl = options.getTtl();
      mTtlAction = TtlAction.fromThrift(options.getTtlAction());
      if (SecurityUtils.isAuthenticationEnabled()) {
        mOwner = SecurityUtils.getOwnerFromThriftClient();
        mGroup = SecurityUtils.getGroupFromThriftClient();
      }
      if (options.isSetMode()) {
        mMode = new Mode(options.getMode());
      } else {
        mMode.applyFileUMask();
      }
    }
  }

  private CreateFileOptions() {
    super();
    mBlockSizeBytes = Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
    mMode.applyFileUMask();
    mCacheable = false;
  }

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @return true if file is cacheable
   */
  public boolean isCacheable() {
    return mCacheable;
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
   * @param cacheable true if the file is cacheable, false otherwise
   * @return the updated options object
   */
  public CreateFileOptions setCacheable(boolean cacheable) {
    mCacheable = cacheable;
    return this;
  }

  @Override
  protected CreateFileOptions getThis() {
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateFileOptions)) {
      return false;
    }
    if (!(super.equals(o))) {
      return false;
    }
    CreateFileOptions that = (CreateFileOptions) o;
    return Objects.equal(mBlockSizeBytes, that.mBlockSizeBytes)
        && Objects.equal(mCacheable, that.mCacheable);
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Objects.hashCode(mBlockSizeBytes, mCacheable);
  }

  @Override
  public String toString() {
    return toStringHelper().add("blockSizeBytes", mBlockSizeBytes)
        .add("cacheable", mCacheable).toString();
  }
}
