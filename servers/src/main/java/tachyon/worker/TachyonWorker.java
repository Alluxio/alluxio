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

package tachyon.worker;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.worker.block.BlockWorker;

/**
 * Entry point for the Tachyon Worker. This class is responsible for initializing the different
 * workers that are configured to run.
 */
public class TachyonWorker {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Main method for Tachyon Worker. A Block Worker will be started and the Tachyon Worker will
   * continue to run until the Block Worker thread exits.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    checkArgs(args);
    TachyonConf tachyonConf = new TachyonConf();
    BlockWorker worker = null;

    try {
      worker = new BlockWorker(tachyonConf);
    } catch (IOException ioe) {
      LOG.error("Failed to initialize the block worker, exiting.", ioe);
      System.exit(-1);
    }

    try {
      worker.process();
    } catch (Exception e) {
      LOG.error("Uncaught exception while running worker, shutting down and exiting.", e);
      try {
        worker.stop();
      } catch (Exception ex) {
        LOG.error("Failed to stop block worker. Exiting.", ex);
      }
      System.exit(-1);
    }

    System.exit(0);
  }

  /**
   * Verifies that no parameters are passed in
   *
   * @param args command line arguments
   */
  private static void checkArgs(String[] args) {
    if (args.length != 0) {
      LOG.info("Usage: java TachyonWorker");
      System.exit(-1);
    }
  }
}
