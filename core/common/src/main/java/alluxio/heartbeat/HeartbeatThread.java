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

package alluxio.heartbeat;

import alluxio.security.LoginUser;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.CommonUtils;
import alluxio.util.SecurityUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Thread class to execute a heartbeat periodically. This thread is daemonic, so it will not prevent
 * the JVM from exiting.
 */
@NotThreadSafe
public final class HeartbeatThread implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatThread.class);

  private final String mThreadName;
  private final HeartbeatExecutor mExecutor;
  private HeartbeatTimer mTimer;

  /**
   * Creates a {@link Runnable} to execute heartbeats for the given {@link HeartbeatExecutor}.
   *
   * This class is responsible for closing the given {@link HeartbeatExecutor} when it finishes.
   *
   * @param threadName identifies the heartbeat thread name
   * @param executor identifies the heartbeat thread executor; an instance of a class that
   *        implements the HeartbeatExecutor interface
   * @param intervalMs Sleep time between different heartbeat
   */
  public HeartbeatThread(String threadName, HeartbeatExecutor executor, long intervalMs) {
    mThreadName = threadName;
    mExecutor = Preconditions.checkNotNull(executor, "executor");
    Class<? extends HeartbeatTimer> timerClass = HeartbeatContext.getTimerClass(threadName);
    try {
      mTimer =
          CommonUtils.createNewClassInstance(timerClass, new Class[] {String.class, long.class},
              new Object[] {threadName, intervalMs});
    } catch (Exception e) {
      String msg = "timer class could not be instantiated";
      LOG.error("{} : {} , {}", msg, threadName, e);
      mTimer = new SleepingTimer(threadName, intervalMs);
    }
  }

  @Override
  public void run() {
    try {
      if (SecurityUtils.isSecurityEnabled() && AuthenticatedClientUser.get() == null) {
        AuthenticatedClientUser.set(LoginUser.get().getName());
      }
    } catch (IOException e) {
      LOG.error("Failed to set AuthenticatedClientUser in HeartbeatThread.");
    }

    // set the thread name
    Thread.currentThread().setName(mThreadName);
    try {
      // Thread.interrupted() clears the interrupt status. Do not call interrupt again to clear it.
      while (!Thread.interrupted()) {
        // TODO(peis): Fix this. The current implementation consumes one thread even when ticking.
        mTimer.tick();
        mExecutor.heartbeat();
      }
    } catch (InterruptedException e) {
      LOG.info("Hearbeat is interrupted.");
    } catch (Exception e) {
      LOG.error("Uncaught exception in heartbeat executor, Heartbeat Thread shutting down", e);
    } finally {
      mExecutor.close();
    }
  }
}
