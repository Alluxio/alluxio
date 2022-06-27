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

package alluxio.fuse.correctness;

import alluxio.fuse.correctness.client.SequentialWriteClient;

import java.util.ArrayList;
import java.util.List;

/**
 * This class validates the sequential write correctness of AlluxioFuse.
 */
public class WriteValidation {
  /**
   * Starts validating sequential write correctness of AlluxioFuse.
   *
   * @param options contains the options for the test
   */
  public static void run(Options options) {
    for (long fileSize : Constants.FILE_SIZES) {
      // Thread and file is one-to-one in writing test
      List<Thread> threads = new ArrayList<>(options.getNumFiles());
      for (int i = 0; i < options.getNumFiles(); i++) {
        Thread t = new Thread(new SequentialWriteClient(options, fileSize));
        threads.add(t);
      }
      for (Thread t: threads) {
        t.start();
        try {
          t.join();
        } catch (InterruptedException e) {
          System.out.println(Constants.THREAD_INTERRUPTED_MESSAGE);
        }
      }
    }
  }
}
