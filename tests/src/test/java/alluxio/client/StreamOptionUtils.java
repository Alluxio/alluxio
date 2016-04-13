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

package alluxio.client;

import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.policy.LocalFirstPolicy;

/**
 * A util class to obtain common CreateFile/OpenFile options for tests.
 */
// TODO(calvin): We can make these methods into constants
public final class StreamOptionUtils {
  private StreamOptionUtils() {
    // not intended for instantiation
  }

  /**
   * Gets CacheThrough {@link CreateFileOptions}.
   *
   * @param conf the Alluxio config
   * @return the {@link CreateFileOptions}
   */
  public static CreateFileOptions getCreateFileOptionsCacheThrough() {
    return CreateFileOptions.defaults().setWriteType(WriteType.CACHE_THROUGH);
  }

  /**
   * Gets MustCache {@link CreateFileOptions}.
   *
   * @param conf the Alluxio config
   * @return the {@link CreateFileOptions}
   */
  public static CreateFileOptions getCreateFileOptionsMustCache() {
    return CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE);
  }

  /**
   * Gets Through {@link CreateFileOptions}.
   *
   * @param conf the Alluxio config
   * @return the {@link CreateFileOptions}
   */
  public static CreateFileOptions getCreateFileOptionsThrough() {
    return CreateFileOptions.defaults().setWriteType(WriteType.THROUGH);
  }

  /**
   * Gets WriteLocal {@link CreateFileOptions}.
   *
   * @param conf the Alluxio config
   * @return the {@link CreateFileOptions}
   */
  public static CreateFileOptions getCreateFileOptionsWriteLocal() {
    return CreateFileOptions.defaults().setWriteType(WriteType.CACHE_THROUGH)
        .setLocationPolicy(new LocalFirstPolicy());
  }

  /**
   * Gets ReadCache {@link OpenFileOptions}.
   *
   * @param conf the Alluxio config
   * @return the {@link OpenFileOptions}
   */
  public static OpenFileOptions getOpenFileOptionsCache() {
    return OpenFileOptions.defaults().setReadType(ReadType.CACHE_PROMOTE);
  }

  /**
   * Gets ReadNoCache {@link OpenFileOptions}.
   *
   * @param conf the Alluxio config
   * @return the {@link OpenFileOptions}
   */
  public static OpenFileOptions getOpenFileOptionsNoCache() {
    return OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
  }

  /**
   * Gets AsyncWrite {@link CreateFileOptions}.
   *
   * @param conf the Alluxio config
   * @return the {@link CreateFileOptions}
   */
  public static CreateFileOptions getCreateFileOptionsAsync() {
    return CreateFileOptions.defaults().setWriteType(WriteType.ASYNC_THROUGH);
  }
}
