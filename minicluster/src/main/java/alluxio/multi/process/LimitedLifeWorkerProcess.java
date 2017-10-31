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

package alluxio.multi.process;

import alluxio.Constants;
import alluxio.worker.AlluxioWorker;

/**
 * Wrapper around AlluxioWorker which will exit after a limited amount of time.
 */
public final class LimitedLifeWorkerProcess {
  /**
   * @param args program arguments
   */
  public static void main(String[] args) {
    Utils.limitLife(Constants.MAX_TEST_PROCESS_LIFETIME_MS);
    AlluxioWorker.main(args);
  }
}
