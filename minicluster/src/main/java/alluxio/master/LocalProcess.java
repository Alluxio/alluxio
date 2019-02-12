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

package alluxio.master;

import alluxio.Process;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * Wrapper around an alluxio process for the purpose of testing.
 *
 * @param <T> the process type
 */
public class LocalProcess<T extends Process> {
  private static final Logger LOG = LoggerFactory.getLogger(LocalProcess.class);

  private final String mName;
  private final Supplier<T> mCreator;

  private T mProcess;
  private Thread mThread;

  /**
   * @param name a name for the process
   * @param creator the supplier for creating the process
   */
  public LocalProcess(String name, Supplier<T> creator) {
    mName = name;
    mCreator = creator;
  }

  /**
   * Starts the process.
   */
  public void start() {
    mProcess = mCreator.get();
    mThread = new Thread(() -> {
      try {
        mProcess.start();
      } catch (InterruptedException e) {
        // this is expected
      } catch (Exception e) {
        // Log the exception as the RuntimeException will be caught and handled silently by JUnit
        LOG.error("Start {} error", mName, e);
        throw new RuntimeException(String.format("Error starting %s: %s", mName, e.toString()), e);
      }
    });
    mThread.setName(mName + "-" + System.identityHashCode(mThread));
    mThread.start();
    TestUtils.waitForReady(mProcess);
  }

  /**
   * Stops the process.
   */
  public void stop() throws Exception {
    if (mThread != null) {
      mProcess.stop();
      while (mThread.isAlive()) {
        LOG.info("Stopping thread {}.", mThread.getName());
        mThread.interrupt();
        mThread.join(1000);
      }
      mThread = null;
    }
  }

  /**
   * @return the process
   */
  public T getProcess() {
    return mProcess;
  }

  /**
   * Waits for the process to be ready.
   */
  public void waitForReady() {
    TestUtils.waitForReady(mProcess);
  }
}
