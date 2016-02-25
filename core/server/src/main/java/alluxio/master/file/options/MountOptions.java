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
import alluxio.master.MasterContext;
import alluxio.proto.journal.File.MountOptionsEntry;
import alluxio.thrift.MountTOptions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method option for mounting.
 */
@NotThreadSafe
public final class MountOptions {
  /**
   * Builder for {@link MountOptions}.
   */
  public static class Builder {
    private boolean mReadOnly;

    /**
     * Creates a new builder for {@link MountOptions}.
     *
     * @param conf an Alluxio configuration
     */
    public Builder(Configuration conf) {
      mReadOnly = false;
    }

    /**
     * @param readOnly the readOnly flag value to use; if true, write or create operations will not
     *                 be allowed under the mount point.
     * @return the builder
     */
    public Builder setReadOnly(boolean readOnly) {
      mReadOnly = readOnly;
      return this;
    }

    /**
     * Builds a new instance of {@link MountOptions}.
     *
     * @return a {@link MountOptions} instance
     */
    public MountOptions build() {
      return new MountOptions(this);
    }
  }

  /**
   * @return the default {@link MountOptions}
   */
  public static MountOptions defaults() {
    return new Builder(MasterContext.getConf()).build();
  }

  private boolean mReadOnly;

  private MountOptions(MountOptions.Builder builder) {
    mReadOnly = builder.mReadOnly;
  }

  /**
   * Creates a new instance of {@link MountOptions} from {@link MountTOptions}.
   *
   * @param options Thrift options
   */
  public MountOptions(MountTOptions options) {
    mReadOnly = options.isReadOnly();
  }

  /**
   * Creates a new instance of {@link MountOptions} from {@link MountOptionsEntry}.
   *
   * @param options Proto options
   */
  public MountOptions(MountOptionsEntry options) {
    if (options.hasReadOnly()) {
      mReadOnly = options.getReadOnly();
    }
  }

  /**
   * @return the readOnly flag; if true, write or create operations will not be allowed under the
   *         mount point.
   */
  public boolean isReadOnly() {
    return mReadOnly;
  }

  /**
   * @return Proto representation of the options
   */
  public MountOptionsEntry toProto() {
    return MountOptionsEntry.newBuilder().setReadOnly(mReadOnly).build();
  }
}
