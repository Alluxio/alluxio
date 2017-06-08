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

import alluxio.wire.LogInfo;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Jdk14Logger;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.Log4jLoggerAdapter;

import java.io.IOException;
import java.lang.reflect.Field;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for working with log.
 */
@ThreadSafe
public final class LogUtils {

  private LogUtils() {} // prevent instantiation

  /**
   * Gets a logger's level with specify name, if the level argument is not null, it will set to
   * specify level first.
   * @param logName logger's name
   * @param level logger's level
   * @return an entity object about the log info
   * @throws IOException if an I/O error occurs
   */
  public static LogInfo setLogLevel(String logName, String level) throws IOException {
    LogInfo result = new LogInfo();
    if (StringUtils.isNotBlank(logName)) {
      result.setLogName(logName);
      Log log = LogFactory.getLog(logName);
      Logger logger = LoggerFactory.getLogger(logName);
      if (log instanceof Log4JLogger) {
        process(((Log4JLogger) log).getLogger(), level, result);
      } else if (log instanceof Jdk14Logger) {
        process(((Jdk14Logger) log).getLogger(), level, result);
      } else if (logger instanceof Log4jLoggerAdapter) {
        try {
          Field field = Log4jLoggerAdapter.class.getDeclaredField("logger");
          field.setAccessible(true);
          org.apache.log4j.Logger log4jLogger = (org.apache.log4j.Logger) field.get(logger);
          process(log4jLogger, level, result);
        } catch (NoSuchFieldException e) {
          result.setMessage(e.getMessage());
        } catch (IllegalAccessException e) {
          result.setMessage(e.getMessage());
        }
      } else {
        result.setMessage("Sorry, " + log.getClass() + " not supported.");
      }
    } else {
      result.setMessage("Please specify a correct logName.");
    }
    return result;
  }

  private static void process(org.apache.log4j.Logger log, String level, LogInfo result)
      throws IOException {
    if (log == null) {
      result.setMessage("log is null.");
      return;
    }
    if (level != null) {
      if (!level.equals(org.apache.log4j.Level.toLevel(level).toString())) {
        result.setMessage("Bad level : " + level);
      } else {
        log.setLevel(org.apache.log4j.Level.toLevel(level));
        result.setMessage("Setting Level to " + level);
      }
    }
    org.apache.log4j.Level lev = null;
    Category category = log;
    while ((category != null) && ((lev = category.getLevel()) == null)) {
      category = category.getParent();
    }
    if (lev != null) {
      result.setLevel(lev.toString());
    }
  }

  private static void process(java.util.logging.Logger log, String level, LogInfo result) throws
      IOException {
    if (log == null) {
      result.setMessage("log is null.");
      return;
    }
    if (level != null) {
      log.setLevel(java.util.logging.Level.parse(level));
      result.setMessage("Setting Level to " + level);
    }

    java.util.logging.Level lev;
    while ((lev = log.getLevel()) == null) {
      log = log.getParent();
    }
    result.setLevel(lev.toString());
  }
}
