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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Reconfigurable;
import alluxio.conf.ReconfigurableRegistry;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.user.UserState;
import alluxio.util.CommonUtils;
import alluxio.util.SecurityUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Thread class to execute a heartbeat periodically. This thread is daemonic, so it will not prevent
 * the JVM from exiting.
 */
@NotThreadSafe
public final class HeartbeatThread implements Runnable, Reconfigurable {
  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatThread.class);

  private final String mThreadName;
  private final HeartbeatExecutor mExecutor;
  private final UserState mUserState;
  private final Supplier<Long> mIntervalSupplier;
  private HeartbeatTimer mTimer;
  private AlluxioConfiguration mConfiguration;
  private Status mStatus;

  /**
   * @param executorName the executor name defined in {@link HeartbeatContext}
   * @param threadId the thread identifier
   * @return the thread name combined of executorName and threadId, if threadId is empty or null,
   *    return executorName
   */
  @VisibleForTesting
  public static String generateThreadName(String executorName, String threadId) {
    if (threadId == null || threadId.isEmpty()) {
      return executorName;
    }
    return executorName + "-" + threadId;
  }

  /**
   * Creates a {@link Runnable} to execute heartbeats for the given {@link HeartbeatExecutor}.
   *
   * This class is responsible for closing the given {@link HeartbeatExecutor} when it finishes.
   *
   * @param executorName identifies the heartbeat thread's executor, should be those defined in
   *    {@link HeartbeatContext}
   * @param threadId the thread identifier, normally, it is empty, but if a heartbeat
   *    executor is started in multiple threads, this can be used to distinguish them, the heartbeat
   *    thread's name is a combination of executorName and threadId
   * @param executor identifies the heartbeat thread executor; an instance of a class that
   *        implements the HeartbeatExecutor interface
   * @param intervalSupplier Sleep time between different heartbeat supplier
   * @param conf Alluxio configuration
   * @param userState the user state for this heartbeat thread
   */
  public HeartbeatThread(String executorName, String threadId, HeartbeatExecutor executor,
      Supplier<Long> intervalSupplier, AlluxioConfiguration conf, UserState userState) {
    mThreadName = generateThreadName(executorName, threadId);
    mExecutor = Preconditions.checkNotNull(executor, "executor");
    Class<? extends HeartbeatTimer> timerClass = HeartbeatContext.getTimerClass(executorName);
    mTimer = CommonUtils.createNewClassInstance(timerClass, new Class[] {String.class, long.class},
        new Object[] {mThreadName, intervalSupplier.get()});
    mConfiguration = conf;
    mUserState = userState;
    mIntervalSupplier = intervalSupplier;
    mStatus = Status.INIT;
    ReconfigurableRegistry.register(this);
  }

  /**
   * Convenience method for
   * {@link
   * #HeartbeatThread(String, String, HeartbeatExecutor, Supplier, AlluxioConfiguration,
   * UserState)} where threadId is null.
   *
   * @param executorName the executor name that is one of those defined in {@link HeartbeatContext}
   * @param executor the heartbeat executor
   * @param intervalSupplier the interval between heartbeats supplier
   * @param conf the Alluxio configuration
   * @param userState the user state for this heartbeat thread
   */
  public HeartbeatThread(String executorName, HeartbeatExecutor executor,
      Supplier<Long> intervalSupplier, AlluxioConfiguration conf, UserState userState) {
    this(executorName, null, executor, intervalSupplier, conf, userState);
  }

  @Override
  public void run() {
    try {
      if (SecurityUtils.isSecurityEnabled(mConfiguration)
          && AuthenticatedClientUser.get(mConfiguration) == null) {
        AuthenticatedClientUser.set(mUserState.getUser().getName());
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
        mStatus = Status.WAITING;
        mTimer.tick();
        mStatus = Status.RUNNING;
        mExecutor.heartbeat();
      }
    } catch (InterruptedException e) {
      // Allow thread to exit.
    } catch (Exception e) {
      LOG.error("Uncaught exception in heartbeat executor, Heartbeat Thread shutting down", e);
    } finally {
      mStatus = Status.STOPPED;
      mExecutor.close();
    }
  }

  /**
   * Updates the heartbeat interval.
   *
   * @param intervalMs the heartbeat interval in ms
   */
  public void updateIntervalMs(long intervalMs) {
    mTimer.setIntervalMs(intervalMs);
  }

  /**
   * @return the status of current heartbeat thread
   */
  public Status getStatus() {
    return mStatus;
  }

  @Override
  public void update() {
    if (mStatus == Status.STOPPED) {
      ReconfigurableRegistry.unregister(this);
      return;
    }
    long interval = mIntervalSupplier.get();
    if (interval != mTimer.getIntervalMs()) {
      updateIntervalMs(interval);
    }
  }

  /**
   * Enum representing the status of HeartbeatThread.
   */
  public enum Status {
    INIT,
    WAITING,
    RUNNING,
    STOPPED,
  }
}
