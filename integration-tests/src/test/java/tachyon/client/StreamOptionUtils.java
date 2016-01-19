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

package tachyon.client;

import tachyon.client.file.options.CreateFileOptions;
import tachyon.client.file.options.OpenFileOptions;
import tachyon.client.file.policy.LocalFirstPolicy;
import tachyon.conf.TachyonConf;

/**
 * A util class to obtain common In/OutStreamOptions for tests
 */
// TODO(calvin): We can make these methods into constants
public final class StreamOptionUtils {
  private StreamOptionUtils() {
    // not intended for instantiation
  }

  /**
   * Gets WriteBoth {@link CreateFileOptions}
   *
   * @param conf the Tachyon config
   * @return the {@link CreateFileOptions}
   */
  public static CreateFileOptions getCreateFileOptionsCacheThrough(TachyonConf conf) {
    return CreateFileOptions.defaults().setWriteType(WriteType.CACHE_THROUGH);
  }

  /**
   * Gets WriteTachyon {@link CreateFileOptions}
   *
   * @param conf the Tachyon config
   * @return the {@link CreateFileOptions}
   */
  public static CreateFileOptions getCreateFileOptionsMustCache(TachyonConf conf) {
    return CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE);
  }

  /**
   * Gets WriteUnderStore {@link CreateFileOptions}
   *
   * @param conf the Tachyon config
   * @return the {@link OutStreamOptions}
   */
  public static CreateFileOptions getCreateFileOptionsThrough(TachyonConf conf) {
    return CreateFileOptions.defaults().setWriteType(WriteType.THROUGH);
  }

  /**
   * Gets WriteLocal {@link CreateFileOptions}
   *
   * @param conf the Tachyon config
   * @return the {@link CreateFileOptions}
   */
  public static CreateFileOptions getCreateFileOptionsWriteLocal(TachyonConf conf) {
    return CreateFileOptions.defaults().setWriteType(WriteType.CACHE_THROUGH)
        .setLocationPolicy(new LocalFirstPolicy());
  }

  /**
   * Gets ReadCache {@link OpenFileOptions}
   *
   * @param conf the Tachyon config
   * @return the InStreamOptions
   */
  public static OpenFileOptions getOpenFileOptionsCache(TachyonConf conf) {
    return OpenFileOptions.defaults().setReadType(ReadType.CACHE_PROMOTE);
  }

  /**
   * Gets ReadNoCache {@link OpenFileOptions}
   *
   * @param conf the Tachyon config
   * @return the {@link OpenFileOptions}
   */
  public static OpenFileOptions getOpenFileOptionsNoCache(TachyonConf conf) {
    return OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
  }

  /**
   * Gets AsyncWrite {@link CreateFileOptions}.
   *
   * @param conf the Tachyon config
   * @return the {@link CreateFileOptions}
   */
  public static CreateFileOptions getCreateFileOptionsAsync(TachyonConf conf) {
    return CreateFileOptions.defaults().setWriteType(WriteType.ASYNC_THROUGH);
  }
}
