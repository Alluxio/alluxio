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

package alluxio.client;

import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.policy.LocalFirstPolicy;

/**
 * A util class to obtain common CreateFile/OpenFile options for tests.
 */
public final class StreamOptionUtils {

  private static final CreateFileOptions WRITE_CACHE_THROUGH =
      getWriteOptions(WriteType.CACHE_THROUGH);
  private static final CreateFileOptions WRITE_CACHE_THROUGH_LOCAL =
      getWriteOptions(WriteType.CACHE_THROUGH).setLocationPolicy(new LocalFirstPolicy());
  private static final CreateFileOptions WRITE_MUST_CACHE =
      getWriteOptions(WriteType.MUST_CACHE);
  private static final CreateFileOptions WRITE_THROUGH = getWriteOptions(WriteType.THROUGH);
  private static final CreateFileOptions WRITE_ASYNC_THROUGH =
      getWriteOptions(WriteType.ASYNC_THROUGH);
  private static final OpenFileOptions READ_CACHE_PROMOTE =
      getReadOptions(ReadType.CACHE_PROMOTE);
  private static final OpenFileOptions READ_NO_CACHE = getReadOptions(ReadType.NO_CACHE);

  private static CreateFileOptions getWriteOptions(final WriteType writeType) {
    return CreateFileOptions.defaults().setWriteType(writeType);
  }

  private static OpenFileOptions getReadOptions(final ReadType readType) {
    return OpenFileOptions.defaults().setReadType(readType);
  }

  private StreamOptionUtils() {
    // not intended for instantiation
  }

  /**
   * Gets CacheThrough {@link CreateFileOptions}.
   *
   * @return the {@link CreateFileOptions}
   */
  public static CreateFileOptions getCreateFileOptionsCacheThrough() {
    return WRITE_CACHE_THROUGH;
  }

  /**
   * Gets MustCache {@link CreateFileOptions}.
   *
   * @return the {@link CreateFileOptions}
   */
  public static CreateFileOptions getCreateFileOptionsMustCache() {
    return WRITE_MUST_CACHE;
  }

  /**
   * Gets Through {@link CreateFileOptions}.
   *
   * @return the {@link CreateFileOptions}
   */
  public static CreateFileOptions getCreateFileOptionsThrough() {
    return WRITE_THROUGH;
  }

  /**
   * Gets WriteLocal {@link CreateFileOptions}.
   *
   * @return the {@link CreateFileOptions}
   */
  public static CreateFileOptions getCreateFileOptionsWriteLocal() {
    return WRITE_CACHE_THROUGH_LOCAL;
  }

  /**
   * Gets ReadCache {@link OpenFileOptions}.
   *
   * @return the {@link OpenFileOptions}
   */
  public static OpenFileOptions getOpenFileOptionsCache() {
    return READ_CACHE_PROMOTE;
  }

  /**
   * Gets ReadNoCache {@link OpenFileOptions}.
   *
   * @return the {@link OpenFileOptions}
   */
  public static OpenFileOptions getOpenFileOptionsNoCache() {
    return READ_NO_CACHE;
  }

  /**
   * Gets AsyncWrite {@link CreateFileOptions}.
   *
   * @return the {@link CreateFileOptions}
   */
  public static CreateFileOptions getCreateFileOptionsAsync() {
    return WRITE_ASYNC_THROUGH;
  }
}
