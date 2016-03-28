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

import alluxio.proto.journal.File;
import alluxio.thrift.MountTOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method option for mounting.
 */
@NotThreadSafe
public final class MountOptions {
  private boolean mReadOnly;
  private Map<String, String> mProperties;

  /**
   * @return the default {@link CompleteFileOptions}
   */
  public static MountOptions defaults() {
    return new MountOptions();
  }

  private MountOptions() {
    mReadOnly = false;
    mProperties = new HashMap<>();
  }

  /**
   * Creates a new instance of {@link MountOptions} from {@link MountTOptions}.
   *
   * @param options Thrift options
   */
  public MountOptions(MountTOptions options) {
    this();
    if (options != null) {
      if (options.isSetReadOnly()) {
        mReadOnly = options.isReadOnly();
      }
      if (options.isSetProperties()) {
        mProperties.putAll(options.getProperties());
      }
    }
  }

  /**
   * Creates a new instance of {@link MountOptions} from {@link File.AddMountPointEntry}.
   *
   * @param options Proto options
   */
  public MountOptions(File.AddMountPointEntry options) {
    this();
    if (options != null) {
      if (options.hasReadOnly()) {
        mReadOnly = options.getReadOnly();
      }
      for (File.StringPairEntry entry : options.getPropertiesList()) {
        mProperties.put(entry.getKey(), entry.getValue());
      }
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
   * @return the properties map
   */
  public Map<String, String> getProperties() {
    return Collections.unmodifiableMap(mProperties);
  }

  /**
   * @param properties the properties map to use. The existing map will be cleared first, and then
   *                   entries of the input map will be added to the internal map.
   * @return the updated options object
   */
  public MountOptions setProperties(Map<String, String> properties) {
    mProperties.clear();
    mProperties.putAll(properties);
    return this;
  }
}
