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

package alluxio.underfs;

import alluxio.file.options.DescendantType;
import alluxio.util.RateLimiter;

import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * The async UFS client interface.
 */
public interface UfsClient {
  /**
   * Gets the ufs status of an object.
   * @param path the path in ufs
   * @param onComplete the callback when the load is complete
   * @param onError the callback when the load encountered an error
   */
  void performGetStatusAsync(
      String path, Consumer<UfsLoadResult> onComplete,
      Consumer<Throwable> onError);

  /**
   * Lists the ufs statuses for a given path.
   * @param path the path in ufs
   * @param continuationToken the continuation token
   * @param startAfter the start after string where the loading starts from
   * @param descendantType the load descendant type (NONE/ONE/ALL)
   * @param onComplete the callback when the load is complete
   * @param onError the callback when the load encountered an error
   */
  void performListingAsync(
      String path, @Nullable String continuationToken, @Nullable String startAfter,
      DescendantType descendantType, Consumer<UfsLoadResult> onComplete,
      Consumer<Throwable> onError);

  /**
   * @return the rate limiter
   */
  RateLimiter getRateLimiter();
}
