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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test utils for UFS.
 */
public class UnderFileSystemTestUtil {
  /**
   * A test helper convert the async performListingAsync call to a sync one.
   * @param ufs the ufs object
   * @param path the path
   * @param descendantType the descendant type
   * @return the ufs load result
   */
  public static UfsLoadResult performListingAsyncAndGetResult(
      UnderFileSystem ufs, String path, DescendantType descendantType) throws Throwable {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> throwable = new AtomicReference<>();
    AtomicReference<UfsLoadResult> result = new AtomicReference<>();
    ufs.performListingAsync(path, null, null, descendantType, descendantType == DescendantType.NONE,
        (r) -> {
          result.set(r);
          latch.countDown();
        }, (t) -> {
          throwable.set(t);
          latch.countDown();
        });
    latch.await();
    if (throwable.get() != null) {
      throw throwable.get();
    }
    return result.get();
  }
}
