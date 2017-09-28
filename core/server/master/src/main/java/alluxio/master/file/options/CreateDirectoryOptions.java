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

import alluxio.Constants;
import alluxio.security.authorization.Mode;
import alluxio.thrift.CreateDirectoryTOptions;
import alluxio.util.SecurityUtils;
import alluxio.wire.ThriftUtils;
import alluxio.wire.TtlAction;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a directory.
 */
@NotThreadSafe
public final class CreateDirectoryOptions extends CreatePathOptions<CreateDirectoryOptions> {
  private boolean mAllowExists;
  private long mTtl;
  private TtlAction mTtlAction;
  /**
   * @return the default {@link CreateDirectoryOptions}
   */
  public static CreateDirectoryOptions defaults() {
    return new CreateDirectoryOptions();
  }

  /**
   * Constructs an instance of {@link CreateDirectoryOptions} from {@link CreateDirectoryTOptions}.
   * The option of permission is constructed with the username obtained from thrift
   * transport.
   *
   * @param options the {@link CreateDirectoryTOptions} to use
   */
  public CreateDirectoryOptions(CreateDirectoryTOptions options) {
    super();
    mAllowExists = options.isAllowExists();
    mPersisted = options.isPersisted();
    mRecursive = options.isRecursive();
    mTtl = options.getTtl();
    mTtlAction = ThriftUtils.fromThrift(options.getTtlAction());
    if (SecurityUtils.isAuthenticationEnabled()) {
      mOwner = SecurityUtils.getOwnerFromThriftClient();
      mGroup = SecurityUtils.getGroupFromThriftClient();
    }
    if (options.isSetMode()) {
      mMode = new Mode(options.getMode());
    } else {
      mMode.applyDirectoryUMask();
    }
  }

  private CreateDirectoryOptions() {
    super();
    mAllowExists = false;
    mTtl = Constants.NO_TTL;
    mTtlAction = TtlAction.DELETE;
    mMode.applyDirectoryUMask();
  }

  /**
   * @return the allowExists flag; it specifies whether an exception should be thrown if the object
   *         being made already exists
   */
  public boolean isAllowExists() {
    return mAllowExists;
  }

  /**
   * @return the TTL (time to live) value; it identifies duration (in seconds) the created directory
   *         should be kept around before it is automatically deleted or free
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
   * @param allowExists the allowExists flag value to use; it specifies whether an exception
   *        should be thrown if the object being made already exists.
   * @return the updated options object
   */
  public CreateDirectoryOptions setAllowExists(boolean allowExists) {
    mAllowExists = allowExists;
    return this;
  }

  /**
   * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created directory should be kept around before it is automatically deleted
   * @return the updated options object
   */
  public CreateDirectoryOptions setTtl(long ttl) {
    mTtl = ttl;
    return getThis();
  }

  /**
   * @param ttlAction the {@link TtlAction}; It informs the action to take when Ttl is expired;
   * @return the updated options object
   */
  public CreateDirectoryOptions setTtlAction(TtlAction ttlAction) {
    mTtlAction = ttlAction;
    return getThis();
  }

  @Override
  protected CreateDirectoryOptions getThis() {
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
    if (!(super.equals(o))) {
      return false;
    }
    CreateDirectoryOptions that = (CreateDirectoryOptions) o;
    return Objects.equal(mAllowExists, that.mAllowExists) && Objects.equal(mTtl, that.mTtl)
        && Objects.equal(mTtlAction, that.mTtlAction);
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Objects.hashCode(mAllowExists, mTtl, mTtlAction);
  }

  @Override
  public String toString() {
    return toStringHelper()
        .add("allowExists", mAllowExists).add("ttl", mTtl)
        .add("ttlAction", mTtlAction).toString();
  }
}
