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
   * Lists the ufs statuses for a given path. The {@link UfsStatus#getName()}
   * function for the returned values should include the full path of each
   * item from the UFS root (not including the bucket name for object stores).
   * It differs from a traditional listing in that if the input variable
   * checkStatus is true, the {@link UfsStatus} for the base path should
   * be included at the start of the results. The function should return
   * immediately, and perform the operation asynchronously.
   * @param path the path in ufs
   * @param continuationToken the continuation token
   * @param startAfter the start after string where the loading starts from
   * @param descendantType the load descendant type (NONE/ONE/ALL)
   * @param checkStatus if true the call will perform a GetStatus on the path
   *                    to see if an object exists, which should be returned
   *                    as part of the result
   * @param onComplete the callback when the load is complete
   * @param onError the callback when the load encountered an error
   */
  void performListingAsync(
      String path, @Nullable String continuationToken, @Nullable String startAfter,
      DescendantType descendantType, boolean checkStatus, Consumer<UfsLoadResult> onComplete,
      Consumer<Throwable> onError);

  /**
   * @return the rate limiter
   */
  RateLimiter getRateLimiter();
}
