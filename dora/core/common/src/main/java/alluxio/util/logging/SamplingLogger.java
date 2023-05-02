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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A logger which limits log message frequency to prevent spamming the logs.
 *
 * After the logger logs a message, it enters a configurable cooldown period where it will ignore
 * repeated logging requests with the same message. Messages are compared by message template, not
 * message arguments.
 */
public class SamplingLogger implements Logger {
  private final Logger mDelegate;
  private final long mCooldownMs;
  private final Map<String, AtomicLong> mCooldowns = new ConcurrentHashMap<>();

  /**
   * @param delegate the logger to log to
   * @param cooldownMs the cooldown period between log messages
   */
  public SamplingLogger(Logger delegate, long cooldownMs) {
    mDelegate = delegate;
    mCooldownMs = cooldownMs;
  }

  private boolean acquireLogPermission(String msg) {
    AtomicLong cooldown = mCooldowns.computeIfAbsent(msg, m -> new AtomicLong(0));
    long allowed = cooldown.get();
    long now = System.currentTimeMillis();
    return now >= allowed && cooldown.compareAndSet(allowed, now + mCooldownMs);
  }

  @Override
  public void trace(String msg) {
    if (isTraceEnabled() && acquireLogPermission(msg)) {
      mDelegate.trace(msg);
    }
  }

  @Override
  public void trace(String format, Object arg) {
    if (isTraceEnabled() && acquireLogPermission(format)) {
      mDelegate.trace(format, arg);
    }
  }

  @Override
  public void trace(String format, Object arg1, Object arg2) {
    if (isTraceEnabled() && acquireLogPermission(format)) {
      mDelegate.trace(format, arg1, arg2);
    }
  }

  @Override
  public void trace(String format, Object... arguments) {
    if (isTraceEnabled() && acquireLogPermission(format)) {
      mDelegate.trace(format, arguments);
    }
  }

  @Override
  public void trace(String msg, Throwable t) {
    if (isTraceEnabled() && acquireLogPermission(msg)) {
      mDelegate.trace(msg, t);
    }
  }

  @Override
  public void trace(Marker marker, String msg) {
    if (isTraceEnabled() && acquireLogPermission(msg)) {
      mDelegate.trace(marker, msg);
    }
  }

  @Override
  public void trace(Marker marker, String format, Object arg) {
    if (isTraceEnabled() && acquireLogPermission(format)) {
      mDelegate.trace(marker, format, arg);
    }
  }

  @Override
  public void trace(Marker marker, String format, Object arg1, Object arg2) {
    if (isTraceEnabled() && acquireLogPermission(format)) {
      mDelegate.trace(marker, format, arg1, arg2);
    }
  }

  @Override
  public void trace(Marker marker, String format, Object... argArray) {
    if (isTraceEnabled() && acquireLogPermission(format)) {
      mDelegate.trace(marker, format, argArray);
    }
  }

  @Override
  public void trace(Marker marker, String msg, Throwable t) {
    if (isTraceEnabled() && acquireLogPermission(msg)) {
      mDelegate.trace(marker, msg, t);
    }
  }

  @Override
  public void debug(String msg) {
    if (isDebugEnabled() && acquireLogPermission(msg)) {
      mDelegate.debug(msg);
    }
  }

  @Override
  public void debug(String format, Object arg) {
    if (isDebugEnabled() && acquireLogPermission(format)) {
      mDelegate.debug(format, arg);
    }
  }

  @Override
  public void debug(String format, Object arg1, Object arg2) {
    if (isDebugEnabled() && acquireLogPermission(format)) {
      mDelegate.debug(format, arg1, arg2);
    }
  }

  @Override
  public void debug(String format, Object... arguments) {
    if (isDebugEnabled() && acquireLogPermission(format)) {
      mDelegate.debug(format, arguments);
    }
  }

  @Override
  public void debug(String msg, Throwable t) {
    if (isDebugEnabled() && acquireLogPermission(msg)) {
      mDelegate.debug(msg, t);
    }
  }

  @Override
  public void debug(Marker marker, String msg) {
    if (isDebugEnabled() && acquireLogPermission(msg)) {
      mDelegate.debug(marker, msg);
    }
  }

  @Override
  public void debug(Marker marker, String format, Object arg) {
    if (isDebugEnabled() && acquireLogPermission(format)) {
      mDelegate.debug(marker, format, arg);
    }
  }

  @Override
  public void debug(Marker marker, String format, Object arg1, Object arg2) {
    if (isDebugEnabled() && acquireLogPermission(format)) {
      mDelegate.debug(marker, format, arg1, arg2);
    }
  }

  @Override
  public void debug(Marker marker, String format, Object... arguments) {
    if (isDebugEnabled() && acquireLogPermission(format)) {
      mDelegate.debug(marker, format, arguments);
    }
  }

  @Override
  public void debug(Marker marker, String msg, Throwable t) {
    if (isDebugEnabled() && acquireLogPermission(msg)) {
      mDelegate.debug(marker, msg, t);
    }
  }

  @Override
  public void info(String msg) {
    if (isInfoEnabled() && acquireLogPermission(msg)) {
      mDelegate.info(msg);
    }
  }

  @Override
  public void info(String format, Object arg) {
    if (isInfoEnabled() && acquireLogPermission(format)) {
      mDelegate.info(format, arg);
    }
  }

  @Override
  public void info(String format, Object arg1, Object arg2) {
    if (isInfoEnabled() && acquireLogPermission(format)) {
      mDelegate.info(format, arg1, arg2);
    }
  }

  @Override
  public void info(String format, Object... arguments) {
    if (isInfoEnabled() && acquireLogPermission(format)) {
      mDelegate.info(format, arguments);
    }
  }

  @Override
  public void info(String msg, Throwable t) {
    if (isInfoEnabled() && acquireLogPermission(msg)) {
      mDelegate.info(msg, t);
    }
  }

  @Override
  public void info(Marker marker, String msg) {
    if (isInfoEnabled() && acquireLogPermission(msg)) {
      mDelegate.info(marker, msg);
    }
  }

  @Override
  public void info(Marker marker, String format, Object arg) {
    if (isInfoEnabled() && acquireLogPermission(format)) {
      mDelegate.info(marker, format, arg);
    }
  }

  @Override
  public void info(Marker marker, String format, Object arg1, Object arg2) {
    if (isInfoEnabled() && acquireLogPermission(format)) {
      mDelegate.info(marker, format, arg1, arg2);
    }
  }

  @Override
  public void info(Marker marker, String format, Object... arguments) {
    if (isInfoEnabled() && acquireLogPermission(format)) {
      mDelegate.info(marker, format, arguments);
    }
  }

  @Override
  public void info(Marker marker, String msg, Throwable t) {
    if (isInfoEnabled() && acquireLogPermission(msg)) {
      mDelegate.info(marker, msg, t);
    }
  }

  @Override
  public void warn(String msg) {
    if (isWarnEnabled() && acquireLogPermission(msg)) {
      mDelegate.warn(msg);
    }
  }

  @Override
  public void warn(String format, Object arg) {
    if (isWarnEnabled() && acquireLogPermission(format)) {
      mDelegate.warn(format, arg);
    }
  }

  @Override
  public void warn(String format, Object... arguments) {
    if (isWarnEnabled() && acquireLogPermission(format)) {
      mDelegate.warn(format, arguments);
    }
  }

  @Override
  public void warn(String format, Object arg1, Object arg2) {
    if (isWarnEnabled() && acquireLogPermission(format)) {
      mDelegate.warn(format, arg1, arg2);
    }
  }

  @Override
  public void warn(String msg, Throwable t) {
    if (isWarnEnabled() && acquireLogPermission(msg)) {
      mDelegate.warn(msg, t);
    }
  }

  @Override
  public void warn(Marker marker, String msg) {
    if (isWarnEnabled() && acquireLogPermission(msg)) {
      mDelegate.warn(marker, msg);
    }
  }

  @Override
  public void warn(Marker marker, String format, Object arg) {
    if (isWarnEnabled() && acquireLogPermission(format)) {
      mDelegate.warn(marker, format, arg);
    }
  }

  @Override
  public void warn(Marker marker, String format, Object arg1, Object arg2) {
    if (isWarnEnabled() && acquireLogPermission(format)) {
      mDelegate.warn(marker, format, arg1, arg2);
    }
  }

  @Override
  public void warn(Marker marker, String format, Object... arguments) {
    if (isWarnEnabled() && acquireLogPermission(format)) {
      mDelegate.warn(marker, format, arguments);
    }
  }

  @Override
  public void warn(Marker marker, String msg, Throwable t) {
    if (isWarnEnabled() && acquireLogPermission(msg)) {
      mDelegate.warn(marker, msg, t);
    }
  }

  @Override
  public void error(String msg) {
    if (isErrorEnabled() && acquireLogPermission(msg)) {
      mDelegate.error(msg);
    }
  }

  @Override
  public void error(String format, Object arg) {
    if (isErrorEnabled() && acquireLogPermission(format)) {
      mDelegate.error(format, arg);
    }
  }

  @Override
  public void error(String format, Object arg1, Object arg2) {
    if (isErrorEnabled() && acquireLogPermission(format)) {
      mDelegate.error(format, arg1, arg2);
    }
  }

  @Override
  public void error(String format, Object... arguments) {
    if (isErrorEnabled() && acquireLogPermission(format)) {
      mDelegate.error(format, arguments);
    }
  }

  @Override
  public void error(String msg, Throwable t) {
    if (isErrorEnabled() && acquireLogPermission(msg)) {
      mDelegate.error(msg, t);
    }
  }

  @Override
  public void error(Marker marker, String msg) {
    if (isErrorEnabled() && acquireLogPermission(msg)) {
      mDelegate.error(marker, msg);
    }
  }

  @Override
  public void error(Marker marker, String format, Object arg) {
    if (isErrorEnabled() && acquireLogPermission(format)) {
      mDelegate.error(marker, format, arg);
    }
  }

  @Override
  public void error(Marker marker, String format, Object arg1, Object arg2) {
    if (isErrorEnabled() && acquireLogPermission(format)) {
      mDelegate.error(marker, format, arg1, arg2);
    }
  }

  @Override
  public void error(Marker marker, String format, Object... arguments) {
    if (isErrorEnabled() && acquireLogPermission(format)) {
      mDelegate.error(marker, format, arguments);
    }
  }

  @Override
  public void error(Marker marker, String msg, Throwable t) {
    if (isErrorEnabled() && acquireLogPermission(msg)) {
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
