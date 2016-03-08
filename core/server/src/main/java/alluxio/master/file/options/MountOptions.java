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

import alluxio.proto.journal.File.MountOptionsEntry;
import alluxio.thrift.MountTOptions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method option for mounting.
 */
@NotThreadSafe
public final class MountOptions {
  private boolean mReadOnly;

  /**
   * @return the default {@link CompleteFileOptions}
   */
  public static MountOptions defaults() {
    return new MountOptions();
  }

  private MountOptions() {
    mReadOnly = false;
  }

  /**
   * Creates a new instance of {@link MountOptions} from {@link MountTOptions}.
   *
   * @param options Thrift options
   */
  public MountOptions(MountTOptions options) {
    this();
    if (options != null && options.isSetReadOnly()) {
      mReadOnly = options.isReadOnly();
    }
  }

  /**
   * Creates a new instance of {@link MountOptions} from {@link MountOptionsEntry}.
   *
   * @param options Proto options
   */
  public MountOptions(MountOptionsEntry options) {
    this();
    if (options != null && options.hasReadOnly()) {
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
   * @param readOnly the readOnly flag to use; if true, write or create operations will not be
   *                 allowed under the mount point.
   * @return the updated options object
   */
  public MountOptions setReadOnly(boolean readOnly) {
    mReadOnly = readOnly;
    return this;
  }

  /**
   * @return Proto representation of the options
   */
  public MountOptionsEntry toProto() {
    return MountOptionsEntry.newBuilder().setReadOnly(mReadOnly).build();
  }
}
