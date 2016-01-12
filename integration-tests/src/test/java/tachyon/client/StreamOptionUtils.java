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

import tachyon.client.file.options.InStreamOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.client.file.policy.LocalFirstPolicy;
import tachyon.conf.TachyonConf;

/**
 * A util class to obtain common In/OutStreamOptions for tests
 *
 */
public final class StreamOptionUtils {
  private StreamOptionUtils() {
    // not intended for instantiation
  }

  /**
   * Gets WriteBoth {@link OutStreamOptions}
   *
   * @param conf the Tachyon config
   * @return the {@link OutStreamOptions}
   */
  public static OutStreamOptions getOutStreamOptionsWriteBoth(TachyonConf conf) {
    return new OutStreamOptions.Builder(conf).setTachyonStorageType(TachyonStorageType.STORE)
               .setUnderStorageType(UnderStorageType.SYNC_PERSIST).build();
  }

  /**
   * Gets WriteTachyon {@link OutStreamOptions}
   *
   * @param conf the Tachyon config
   * @return the {@link OutStreamOptions}
   */
  public static OutStreamOptions getOutStreamOptionsWriteTachyon(TachyonConf conf) {
    return new OutStreamOptions.Builder(conf).setTachyonStorageType(TachyonStorageType.STORE)
               .setUnderStorageType(UnderStorageType.NO_PERSIST).build();
  }

  /**
   * Gets WriteUnderStore {@link OutStreamOptions}
   *
   * @param conf the Tachyon config
   * @return the {@link OutStreamOptions}
   */
  public static OutStreamOptions getOutStreamOptionsWriteUnderStore(TachyonConf conf) {
    return new OutStreamOptions.Builder(conf).setTachyonStorageType(TachyonStorageType.NO_STORE)
               .setUnderStorageType(UnderStorageType.SYNC_PERSIST).build();
  }

  /**
   * Gets WriteLocal {@link OutStreamOptions}
   *
   * @param conf the Tachyon config
   * @return the {@link OutStreamOptions}
   */
  public static OutStreamOptions getOutStreamOptionsWriteLocal(TachyonConf conf) {
    return new OutStreamOptions.Builder(conf).setTachyonStorageType(TachyonStorageType.STORE)
        .setUnderStorageType(UnderStorageType.SYNC_PERSIST)
        .setLocationPolicy(new LocalFirstPolicy()).build();
  }

  /**
   * Gets ReadCache {@link InStreamOptions}
   *
   * @param conf the Tachyon config
   * @return the InStreamOptions
   */
  public static InStreamOptions getInStreamOptionsReadCache(TachyonConf conf) {
    return new InStreamOptions.Builder(conf).setTachyonStorageType(TachyonStorageType.STORE)
        .build();
  }

  /**
   * Gets ReadNoCache {@link InStreamOptions}
   *
   * @param conf the Tachyon config
   * @return the {@link InStreamOptions}
   */
  public static InStreamOptions getInStreamOptionsReadNoCache(TachyonConf conf) {
    return new InStreamOptions.Builder(conf).setTachyonStorageType(TachyonStorageType.NO_STORE)
        .build();
  }

  /**
   * Gets AsyncWrite {@link OutStreamOptions}.
   *
   * @param conf the Tachyon config
   * @return the {@link OutStreamOptions}
   */
  public static OutStreamOptions getOutStreamOptionsWriteAsync(TachyonConf conf) {
    return new OutStreamOptions.Builder().setTachyonStorageType(TachyonStorageType.STORE)
        .setUnderStorageType(UnderStorageType.ASYNC_PERSIST).build();
  }
}
