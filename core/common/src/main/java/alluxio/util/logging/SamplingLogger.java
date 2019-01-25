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

package alluxio.util.logging;

import org.slf4j.Logger;
import org.slf4j.Marker;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A logger which limits log message frequency to prevent spamming the logs.
 *
 * After the logger logs a message, it enters a configurable cooldown period where it will ignore
 * logging requests.
 */
public class SamplingLogger implements Logger {
  private final Logger mDelegate;
  private final long mCooldownMs;
  private final AtomicLong mNextAllowedLog = new AtomicLong(0);

  /**
   * @param delegate the logger to log to
   * @param cooldownMs the cooldown period between log messages
   */
  public SamplingLogger(Logger delegate, long cooldownMs) {
    mDelegate = delegate;
    mCooldownMs = cooldownMs;
  }

  private boolean acquireLogPermission() {
    long allowed = mNextAllowedLog.get();
    long now = System.currentTimeMillis();
    return now >= allowed && mNextAllowedLog.compareAndSet(allowed, now + mCooldownMs);
  }

  @Override
  public void trace(String msg) {
    if (acquireLogPermission()) {
      mDelegate.trace(msg);
    }
  }

  @Override
  public void trace(String format, Object arg) {
    if (acquireLogPermission()) {
      mDelegate.trace(format, arg);
    }
  }

  @Override
  public void trace(String format, Object arg1, Object arg2) {
    if (acquireLogPermission()) {
      mDelegate.trace(format, arg1, arg2);
    }
  }

  @Override
  public void trace(String format, Object... arguments) {
    if (acquireLogPermission()) {
      mDelegate.trace(format, arguments);
    }
  }

  @Override
  public void trace(String msg, Throwable t) {
    if (acquireLogPermission()) {
      mDelegate.trace(msg, t);
    }
  }

  @Override
  public void trace(Marker marker, String msg) {
    if (acquireLogPermission()) {
      mDelegate.trace(marker, msg);
    }
  }

  @Override
  public void trace(Marker marker, String format, Object arg) {
    if (acquireLogPermission()) {
      mDelegate.trace(marker, format, arg);
    }
  }

  @Override
  public void trace(Marker marker, String format, Object arg1, Object arg2) {
    if (acquireLogPermission()) {
      mDelegate.trace(marker, format, arg1, arg2);
    }
  }

  @Override
  public void trace(Marker marker, String format, Object... argArray) {
    if (acquireLogPermission()) {
      mDelegate.trace(marker, format, argArray);
    }
  }

  @Override
  public void trace(Marker marker, String msg, Throwable t) {
    if (acquireLogPermission()) {
      mDelegate.trace(marker, msg, t);
    }
  }

  @Override
  public void debug(String msg) {
    if (acquireLogPermission()) {
      mDelegate.debug(msg);
    }
  }

  @Override
  public void debug(String format, Object arg) {
    if (acquireLogPermission()) {
      mDelegate.debug(format, arg);
    }
  }

  @Override
  public void debug(String format, Object arg1, Object arg2) {
    if (acquireLogPermission()) {
      mDelegate.debug(format, arg1, arg2);
    }
  }

  @Override
  public void debug(String format, Object... arguments) {
    if (acquireLogPermission()) {
      mDelegate.debug(format, arguments);
    }
  }

  @Override
  public void debug(String msg, Throwable t) {
    if (acquireLogPermission()) {
      mDelegate.debug(msg, t);
    }
  }

  @Override
  public void debug(Marker marker, String msg) {
    if (acquireLogPermission()) {
      mDelegate.debug(marker, msg);
    }
  }

  @Override
  public void debug(Marker marker, String format, Object arg) {
    if (acquireLogPermission()) {
      mDelegate.debug(marker, format, arg);
    }
  }

  @Override
  public void debug(Marker marker, String format, Object arg1, Object arg2) {
    if (acquireLogPermission()) {
      mDelegate.debug(marker, format, arg1, arg2);
    }
  }

  @Override
  public void debug(Marker marker, String format, Object... arguments) {
    if (acquireLogPermission()) {
      mDelegate.debug(marker, format, arguments);
    }
  }

  @Override
  public void debug(Marker marker, String msg, Throwable t) {
    if (acquireLogPermission()) {
      mDelegate.debug(marker, msg, t);
    }
  }

  @Override
  public void info(String msg) {
    if (acquireLogPermission()) {
      mDelegate.info(msg);
    }
  }

  @Override
  public void info(String format, Object arg) {
    if (acquireLogPermission()) {
      mDelegate.info(format, arg);
    }
  }

  @Override
  public void info(String format, Object arg1, Object arg2) {
    if (acquireLogPermission()) {
      mDelegate.info(format, arg1, arg2);
    }
  }

  @Override
  public void info(String format, Object... arguments) {
    if (acquireLogPermission()) {
      mDelegate.info(format, arguments);
    }
  }

  @Override
  public void info(String msg, Throwable t) {
    if (acquireLogPermission()) {
      mDelegate.info(msg, t);
    }
  }

  @Override
  public void info(Marker marker, String msg) {
    if (acquireLogPermission()) {
      mDelegate.info(marker, msg);
    }
  }

  @Override
  public void info(Marker marker, String format, Object arg) {
    if (acquireLogPermission()) {
      mDelegate.info(marker, format, arg);
    }
  }

  @Override
  public void info(Marker marker, String format, Object arg1, Object arg2) {
    if (acquireLogPermission()) {
      mDelegate.info(marker, format, arg1, arg2);
    }
  }

  @Override
  public void info(Marker marker, String format, Object... arguments) {
    if (acquireLogPermission()) {
      mDelegate.info(marker, format, arguments);
    }
  }

  @Override
  public void info(Marker marker, String msg, Throwable t) {
    if (acquireLogPermission()) {
      mDelegate.info(marker, msg, t);
    }
  }

  @Override
  public void warn(String msg) {
    if (acquireLogPermission()) {
      mDelegate.warn(msg);
    }
  }

  @Override
  public void warn(String format, Object arg) {
    if (acquireLogPermission()) {
      mDelegate.warn(format, arg);
    }
  }

  @Override
  public void warn(String format, Object... arguments) {
    if (acquireLogPermission()) {
      mDelegate.warn(format, arguments);
    }
  }

  @Override
  public void warn(String format, Object arg1, Object arg2) {
    if (acquireLogPermission()) {
      mDelegate.warn(format, arg1, arg2);
    }
  }

  @Override
  public void warn(String msg, Throwable t) {
    if (acquireLogPermission()) {
      mDelegate.warn(msg, t);
    }
  }

  @Override
  public void warn(Marker marker, String msg) {
    if (acquireLogPermission()) {
      mDelegate.warn(marker, msg);
    }
  }

  @Override
  public void warn(Marker marker, String format, Object arg) {
    if (acquireLogPermission()) {
      mDelegate.warn(marker, format, arg);
    }
  }

  @Override
  public void warn(Marker marker, String format, Object arg1, Object arg2) {
    if (acquireLogPermission()) {
      mDelegate.warn(marker, format, arg1, arg2);
    }
  }

  @Override
  public void warn(Marker marker, String format, Object... arguments) {
    if (acquireLogPermission()) {
      mDelegate.warn(marker, format, arguments);
    }
  }

  @Override
  public void warn(Marker marker, String msg, Throwable t) {
    if (acquireLogPermission()) {
      mDelegate.warn(marker, msg, t);
    }
  }

  @Override
  public void error(String msg) {
    if (acquireLogPermission()) {
      mDelegate.error(msg);
    }
  }

  @Override
  public void error(String format, Object arg) {
    if (acquireLogPermission()) {
      mDelegate.error(format, arg);
    }
  }

  @Override
  public void error(String format, Object arg1, Object arg2) {
    if (acquireLogPermission()) {
      mDelegate.error(format, arg1, arg2);
    }
  }

  @Override
  public void error(String format, Object... arguments) {
    if (acquireLogPermission()) {
      mDelegate.error(format, arguments);
    }
  }

  @Override
  public void error(String msg, Throwable t) {
    if (acquireLogPermission()) {
      mDelegate.error(msg, t);
    }
  }

  @Override
  public void error(Marker marker, String msg) {
    if (acquireLogPermission()) {
      mDelegate.error(marker, msg);
    }
  }

  @Override
  public void error(Marker marker, String format, Object arg) {
    if (acquireLogPermission()) {
      mDelegate.error(marker, format, arg);
    }
  }

  @Override
  public void error(Marker marker, String format, Object arg1, Object arg2) {
    if (acquireLogPermission()) {
      mDelegate.error(marker, format, arg1, arg2);
    }
  }

  @Override
  public void error(Marker marker, String format, Object... arguments) {
    if (acquireLogPermission()) {
      mDelegate.error(marker, format, arguments);
    }
  }

  @Override
  public void error(Marker marker, String msg, Throwable t) {
    if (acquireLogPermission()) {
      mDelegate.error(marker, msg, t);
    }
  }

  @Override
  public String getName() {
    return mDelegate.getName();
  }

  @Override
  public boolean isTraceEnabled() {
    return mDelegate.isTraceEnabled();
  }

  @Override
  public boolean isTraceEnabled(Marker marker) {
    return mDelegate.isTraceEnabled(marker);
  }

  @Override
  public boolean isDebugEnabled() {
    return mDelegate.isDebugEnabled();
  }

  @Override
  public boolean isDebugEnabled(Marker marker) {
    return mDelegate.isDebugEnabled(marker);
  }

  @Override
  public boolean isInfoEnabled() {
    return mDelegate.isInfoEnabled();
  }

  @Override
  public boolean isInfoEnabled(Marker marker) {
    return mDelegate.isInfoEnabled(marker);
  }

  @Override
  public boolean isWarnEnabled() {
    return mDelegate.isWarnEnabled();
  }

  @Override
  public boolean isWarnEnabled(Marker marker) {
    return mDelegate.isWarnEnabled(marker);
  }

  @Override
  public boolean isErrorEnabled() {
    return mDelegate.isErrorEnabled();
  }

  @Override
  public boolean isErrorEnabled(Marker marker) {
    return mDelegate.isErrorEnabled(marker);
  }
}
