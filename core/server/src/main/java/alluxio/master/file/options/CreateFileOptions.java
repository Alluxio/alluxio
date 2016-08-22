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
import alluxio.TtlExpiryAction;
import alluxio.security.authorization.Permission;
import alluxio.thrift.CreateFileTOptions;

import com.google.common.base.Objects;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a file.
 */
@NotThreadSafe
public final class CreateFileOptions extends CreatePathOptions<CreateFileOptions> {
  private long mBlockSizeBytes;
  private long mTtl;
  private TtlExpiryAction mTtlExpiryAction;

  /**
   * @return the default {@link CreateFileOptions}
   */
  public static CreateFileOptions defaults() {
    return new CreateFileOptions();
  }

  /**
   * Constructs an instance of {@link CreateFileOptions} from {@link CreateFileTOptions}. The
   * option of permission is constructed with the username obtained from thrift transport.
   *
   * @param options the {@link CreateFileTOptions} to use
   * @throws IOException if it failed to retrieve users or groups from thrift transport
   */
  public CreateFileOptions(CreateFileTOptions options) throws IOException {
    super();
    mBlockSizeBytes = options.getBlockSizeBytes();
    mPersisted = options.isPersisted();
    mRecursive = options.isRecursive();
    mTtl = options.getTtl();
    mTtlExpiryAction = (options.getTtlExpiryAction() == alluxio.thrift.TtlExpiryAction.Free)
        ? TtlExpiryAction.FREE : TtlExpiryAction.DELETE;
    mPermission = Permission.defaults().setOwnerFromThriftClient();
  }

  private CreateFileOptions() {
    super();
    mBlockSizeBytes = Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
    mTtl = Constants.NO_TTL;
    mTtlExpiryAction = TtlExpiryAction.DELETE;
  }

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @return the TTL (time to live) value; it identifies duration (in seconds) the created file
   *         should be kept around before it is automatically deleted
   */
  public long getTtl() {
    return mTtl;
  }

  /**
   * @return the {@link TtlExpiryAction}; It informs the action to take when Ttl is expired. It can
   *         be either DELETE/FREE.git che
   */
  public TtlExpiryAction getTtlExpiryAction() {
    return mTtlExpiryAction;
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
   * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted
   * @return the updated options object
   */
  public CreateFileOptions setTtl(long ttl) {
    mTtl = ttl;
    return getThis();
  }

  /**
   * @param ttlExpiryAction the {@link TtlExpiryAction};
   *        It informs the action to take when Ttl is expired;
   * @return the updated options object
   */
  public CreateFileOptions setTtlExpiryAction(TtlExpiryAction ttlExpiryAction) {
    mTtlExpiryAction = ttlExpiryAction;
    return getThis();
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
        && Objects.equal(mTtl, that.mTtl)
        && Objects.equal(mTtlExpiryAction, that.mTtlExpiryAction);
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Objects.hashCode(mBlockSizeBytes, mTtl, mTtlExpiryAction);
  }

  @Override
  public String toString() {
    return toStringHelper()
        .add("blockSizeBytes", mBlockSizeBytes)
        .add("ttl", mTtl)
        .add("ttlExpiryAction", mTtlExpiryAction)
        .toString();
  }
}
