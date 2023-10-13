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

import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Reconfigurable;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ReconfigurableRegistry;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.user.UserState;
import alluxio.util.CommonUtils;
import alluxio.util.SecurityUtils;
import alluxio.wire.HeartbeatThreadInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Thread class to execute a heartbeat periodically. This thread is daemonic, so it will not prevent
 * the JVM from exiting.
 */
@NotThreadSafe
public final class HeartbeatThread implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatThread.class);
  private static final String DATE_PATTERN =
      Configuration.getString(PropertyKey.USER_DATE_FORMAT_PATTERN);

  private final String mThreadName;
  private final HeartbeatExecutor mExecutor;
  private final UserState mUserState;
  private HeartbeatTimer mTimer;
  private AlluxioConfiguration mConfiguration;
  private volatile Status mStatus;
  private AtomicLong mCounter = new AtomicLong(0L);
  private volatile long mStartTickTime;
  private volatile long mStartHeartbeatTime;
  private volatile long mEndHeartbeatTime;

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
   * @param clock the clock used to compute the current time
   */
  public HeartbeatThread(String executorName, String threadId, HeartbeatExecutor executor,
      Supplier<SleepIntervalSupplier> intervalSupplier,
      AlluxioConfiguration conf, UserState userState, Clock clock) {
    mThreadName = generateThreadName(executorName, threadId);
    mExecutor = Preconditions.checkNotNull(executor, "executor");
    Class<? extends HeartbeatTimer> timerClass = HeartbeatContext.getTimerClass(executorName);
    mTimer = CommonUtils.createNewClassInstance(timerClass,
        new Class[] {String.class, Clock.class, Supplier.class},
        new Object[] {mThreadName, clock, intervalSupplier});
    mConfiguration = conf;
    mUserState = userState;
    mStatus = Status.INIT;
    if (mTimer instanceof Reconfigurable) {
      ReconfigurableRegistry.register((Reconfigurable) mTimer);
    }
  }

  /**
   * Convenience method for
   * {@link
   * #HeartbeatThread(String, String, HeartbeatExecutor, Supplier, AlluxioConfiguration,
   * UserState, Clock)} where threadId is null.
   *
   * @param executorName the executor name that is one of those defined in {@link HeartbeatContext}
   * @param executor the heartbeat executor
   * @param intervalSupplier the interval between heartbeats supplier
   * @param conf the Alluxio configuration
   * @param userState the user state for this heartbeat thread
   */
  public HeartbeatThread(String executorName, HeartbeatExecutor executor,
      Supplier<SleepIntervalSupplier> intervalSupplier, AlluxioConfiguration conf,
      UserState userState) {
    this(executorName, null, executor, intervalSupplier, conf, userState, Clock.systemUTC());
  }

  /**
   * Convenience method for
   * {@link
   * #HeartbeatThread(String, String, HeartbeatExecutor, Supplier, AlluxioConfiguration,
   * UserState, Clock)} where threadId is null.
   *
   * @param executorName the executor name that is one of those defined in {@link HeartbeatContext}
   * @param executor the heartbeat executor
   * @param intervalSupplier the interval between heartbeats supplier
   * @param conf the Alluxio configuration
   * @param userState the user state for this heartbeat thread
   * @param clock the clock used to compute the current time
   */
  public HeartbeatThread(String executorName, HeartbeatExecutor executor,
      Supplier<SleepIntervalSupplier> intervalSupplier,
      AlluxioConfiguration conf, UserState userState, Clock clock) {
    this(executorName, null, executor, intervalSupplier,
        conf, userState, clock);
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
        mStartTickTime = CommonUtils.getCurrentMs();
        long limitTime = mTimer.tick();
        mStatus = Status.RUNNING;
        mStartHeartbeatTime = CommonUtils.getCurrentMs();
        LOG.debug("{} #{} will run limited in {}s", mThreadName, mCounter.get(),
            limitTime / Constants.SECOND_MS);
        mExecutor.heartbeat(limitTime);
        mEndHeartbeatTime = CommonUtils.getCurrentMs();
        updateState();
      }
    } catch (InterruptedException e) {
      // Allow thread to exit.
    } catch (Exception e) {
      LOG.error("Uncaught exception in heartbeat executor, Heartbeat Thread shutting down", e);
    } finally {
      mStatus = Status.STOPPED;
      if (mTimer instanceof Reconfigurable) {
        ReconfigurableRegistry.unregister((Reconfigurable) mTimer);
      }
      mExecutor.close();
    }
  }

  private synchronized void updateState() {
    mCounter.incrementAndGet();
    mStartTickTime = 0L;
    mStartHeartbeatTime = 0L;
  }

  /**
   * @return the thread name
   */
  public String getThreadName() {
    return mThreadName;
  }

  /**
   * @return the {@link HeartbeatThreadInfo} for the heartbeat thread
   */
  public synchronized HeartbeatThreadInfo toHeartbeatThreadInfo() {
    String previousReport = String.format("#%d [%s - %s - %s] ticked(s) %d, run(s) %d.",
        mCounter.get(),
        CommonUtils.convertMsToDate(mStartTickTime, DATE_PATTERN),
        CommonUtils.convertMsToDate(mStartHeartbeatTime, DATE_PATTERN),
        CommonUtils.convertMsToDate(mEndHeartbeatTime, DATE_PATTERN),
        (mStartHeartbeatTime - mStartTickTime) / Constants.SECOND_MS,
        (mEndHeartbeatTime - mStartHeartbeatTime) / Constants.SECOND_MS);
    HeartbeatThreadInfo heartbeatThreadInfo = new HeartbeatThreadInfo()
        .setThreadName(mThreadName)
        .setCount(mCounter.get())
        .setStatus(mStatus)
        .setPreviousReport(previousReport);
    if (mStartTickTime != 0) {
      heartbeatThreadInfo.setStartTickTime(
          CommonUtils.convertMsToDate(mStartTickTime, DATE_PATTERN));
    }
    if (mStartHeartbeatTime != 0) {
      heartbeatThreadInfo.setStartHeartbeatTime(
          CommonUtils.convertMsToDate(mStartHeartbeatTime, DATE_PATTERN));
    }
    return heartbeatThreadInfo;
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
