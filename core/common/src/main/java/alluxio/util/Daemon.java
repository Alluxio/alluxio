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

package alluxio.util;

/**
 * A thread that has called {@link Thread#setDaemon(boolean) } with true.
 */
public class Daemon extends Thread {

  {
    setDaemon(true);
  }

  Runnable mRunnable = null;

  /**
   * Constructs a daemon thread.
   */
  public Daemon() {
    super();
  }

  /**
   * Constructs a daemon thread.
   *
   * @param runnable given a Runnable object
   */
  public Daemon(Runnable runnable) {
    super(runnable);
    mRunnable = runnable;
    setName(((Object) mRunnable).toString());
  }

  /**
   * @return mRunnable
   */
  public Runnable getRunnable() {
    return mRunnable;
  }
}

