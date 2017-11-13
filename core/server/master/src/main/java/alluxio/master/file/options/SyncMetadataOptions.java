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

import alluxio.thrift.SyncMetadataTOptions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for syncing the metadata of a path.
 */
@NotThreadSafe
public final class SyncMetadataOptions extends CommonOptions {
  /**
   * @return the default {@link SyncMetadataOptions}
   */
  public static SyncMetadataOptions defaults() {
    return new SyncMetadataOptions();
  }

  /**
   * Constructs an instance of {@link SyncMetadataOptions} from
   * {@link SyncMetadataTOptions}.
   *
   * @param options the {@link SyncMetadataTOptions} to use
   */
  public SyncMetadataOptions(SyncMetadataTOptions options) {
    super(options != null ? options.getCommonOptions() : null);
  }

  private SyncMetadataOptions() {
    this(null);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SyncMetadataOptions)) {
      return false;
    }
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public String toString() {
    return toStringHelper()
        .toString();
  }
}
