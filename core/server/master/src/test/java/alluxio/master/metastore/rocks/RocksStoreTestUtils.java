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

package alluxio.master.metastore.rocks;

import static org.junit.Assert.fail;

import java.util.List;
import java.util.concurrent.Future;

public class RocksStoreTestUtils {
  public static void waitForReaders(List<Future<Void>> futures) {
    futures.stream().forEach(f -> {
      try {
        f.get();
      } catch (Exception e) {
        fail("Met uncaught exception from iteration");
      }
    });
  }
}
