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

import java.util.concurrent.ThreadFactory;

/**
 *A thread that has called {@link Thread#setDaemon(boolean) } with true.
 */
public class Daemon extends Thread {

  {
    setDaemon(true);
  }

  /**
   * Provide a factory for named daemon threads,
   * for use in ExecutorServices constructors.
   */
  public static class DaemonFactory extends Daemon implements ThreadFactory {

    @Override
    public Thread newThread(Runnable runnable) {
      return new Daemon(runnable);
    }
  }

  Runnable mRunnable = null;

  /**
   * Construct a daemon thread.
   */
  public Daemon() {
    super();
  }

  /** Construct a daemon thread.
   * @param runnable given a Runnable object
   */
  public Daemon(Runnable runnable) {
    super(runnable);
    mRunnable = runnable;
    setName(((Object) mRunnable).toString());
  }

  /** Construct a daemon thread to be part of a specified thread group.
   * @param group thread group
   * @param runnable an Runnable object
   */
  public Daemon(ThreadGroup group, Runnable runnable) {
    super(group, runnable);
    mRunnable = runnable;
    setName(((Object) runnable).toString());
  }

  /**
   * @return mRunnable
   */
  public Runnable getRunnable() {
    return mRunnable;
  }
}

