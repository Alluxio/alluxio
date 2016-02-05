/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.client;

import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.policy.LocalFirstPolicy;
import alluxio.Configuration;

/**
 * A util class to obtain common CreateFile/OpenFile options for tests
 */
// TODO(calvin): We can make these methods into constants
public final class StreamOptionUtils {
  private StreamOptionUtils() {
    // not intended for instantiation
  }

  /**
   * Gets CacheThrough {@link CreateFileOptions}
   *
   * @param conf the Alluxio config
   * @return the {@link CreateFileOptions}
   */
  public static CreateFileOptions getCreateFileOptionsCacheThrough(Configuration conf) {
    return CreateFileOptions.defaults().setWriteType(WriteType.CACHE_THROUGH);
  }

  /**
   * Gets MustCache {@link CreateFileOptions}
   *
   * @param conf the Alluxio config
   * @return the {@link CreateFileOptions}
   */
  public static CreateFileOptions getCreateFileOptionsMustCache(Configuration conf) {
    return CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE);
  }

  /**
   * Gets Through {@link CreateFileOptions}
   *
   * @param conf the Alluxio config
   * @return the {@link CreateFileOptions}
   */
  public static CreateFileOptions getCreateFileOptionsThrough(Configuration conf) {
    return CreateFileOptions.defaults().setWriteType(WriteType.THROUGH);
  }

  /**
   * Gets WriteLocal {@link CreateFileOptions}
   *
   * @param conf the Alluxio config
   * @return the {@link CreateFileOptions}
   */
  public static CreateFileOptions getCreateFileOptionsWriteLocal(Configuration conf) {
    return CreateFileOptions.defaults().setWriteType(WriteType.CACHE_THROUGH)
        .setLocationPolicy(new LocalFirstPolicy());
  }

  /**
   * Gets ReadCache {@link OpenFileOptions}
   *
   * @param conf the Alluxio config
   * @return the {@link OpenFileOptions}
   */
  public static OpenFileOptions getOpenFileOptionsCache(Configuration conf) {
    return OpenFileOptions.defaults().setReadType(ReadType.CACHE_PROMOTE);
  }

  /**
   * Gets ReadNoCache {@link OpenFileOptions}
   *
   * @param conf the Alluxio config
   * @return the {@link OpenFileOptions}
   */
  public static OpenFileOptions getOpenFileOptionsNoCache(Configuration conf) {
    return OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
  }

  /**
   * Gets AsyncWrite {@link CreateFileOptions}.
   *
   * @param conf the Alluxio config
   * @return the {@link CreateFileOptions}
   */
  public static CreateFileOptions getCreateFileOptionsAsync(Configuration conf) {
    return CreateFileOptions.defaults().setWriteType(WriteType.ASYNC_THROUGH);
  }
}
