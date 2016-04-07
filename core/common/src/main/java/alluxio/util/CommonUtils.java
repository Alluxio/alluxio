/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.util;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.security.group.GroupMappingService;
import alluxio.util.ShellUtils.ExitCodeException;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Common utilities shared by all components in Alluxio.
 */
@ThreadSafe
public final class CommonUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final Random RANDOM = new Random();

  /**
   * @return current time in milliseconds
   */
  public static long getCurrentMs() {
    return System.currentTimeMillis();
  }

  /**
   * Converts a list of objects to a string.
   *
   * @param list list of objects
   * @param <T> type of the objects
   * @return space-separated concatenation of the string representation returned by Object#toString
   *         of the individual objects
   */
  public static <T> String listToString(List<T> list) {
    StringBuilder sb = new StringBuilder();
    for (T s : list) {
      if (sb.length() != 0) {
        sb.append(" ");
      }
      sb.append(s);
    }
    return sb.toString();
  }

  /**
   * Converts varargs of objects to a string.
   *
   * @param separator separator string
   * @param args variable arguments
   * @param <T> type of the objects
   * @return concatenation of the string representation returned by Object#toString
   *         of the individual objects
   */
  public static <T> String argsToString(String separator, T... args) {
    StringBuilder sb = new StringBuilder();
    for (T s : args) {
      if (sb.length() != 0) {
        sb.append(separator);
      }
      sb.append(s);
    }
    return sb.toString();
  }

  /**
   * Parses {@code ArrayList<String>} into {@code String[]}.
   *
   * @param src is the ArrayList of strings
   * @return an array of strings
   */
  public static String[] toStringArray(ArrayList<String> src) {
    String[] ret = new String[src.size()];
    return src.toArray(ret);
  }

  /**
   * Generates a random string of the given length.
   *
   * @param length the length
   * @return a random string
   */
  public static String randomString(int length) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      sb.append((char) (RANDOM.nextInt(96) + 32)); // generates a random printable character
    }
    return sb.toString();
  }

  /**
   * Generates a random byte array of the given length.
   *
   * @param length the length
   * @return a random byte array
   */
  public static byte[] randomBytes(int length) {
    byte[] result = new byte[length];
    RANDOM.nextBytes(result);
    return result;
  }

  /**
   * Sleeps for the given number of milliseconds.
   *
   * @param timeMs sleep duration in milliseconds
   */
  public static void sleepMs(long timeMs) {
    sleepMs(null, timeMs);
  }

  /**
   * Sleeps for the given number of milliseconds, reporting interruptions using the given logger.
   *
   * @param logger logger for reporting interruptions
   * @param timeMs sleep duration in milliseconds
   */
  public static void sleepMs(Logger logger, long timeMs) {
    sleepMs(logger, timeMs, false);
  }

  /**
   * Sleeps for the given number of milliseconds, reporting interruptions using the given logger,
   * and optionally pass the interruption to the caller.
   *
   * @param logger logger for reporting interruptions
   * @param timeMs sleep duration in milliseconds
   * @param shouldInterrupt determines if interruption should be passed to the caller
   */
  public static void sleepMs(Logger logger, long timeMs, boolean shouldInterrupt) {
    try {
      Thread.sleep(timeMs);
    } catch (InterruptedException e) {
      // The null check is needed otherwise #sleeMs(long) will cause a NullPointerException
      // if the thread is interrupted
      if (logger != null) {
        logger.warn(e.getMessage(), e);
      }
      if (shouldInterrupt) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Common empty loop utility that serves the purpose of warming up the JVM before performance
   * microbenchmarks.
   */
  public static void warmUpLoop() {
    for (int k = 0; k < 10000000; k++) {}
  }

  /**
   * Creates new instance of a class by calling a constructor that receives ctorClassArgs arguments.
   *
   * @param <T> type of the object
   * @param cls the class to create
   * @param ctorClassArgs parameters type list of the constructor to initiate, if null default
   *        constructor will be called
   * @param ctorArgs the arguments to pass the constructor
   * @return new class object or null if not successful
   * @throws InstantiationException if the instantiation fails
   * @throws IllegalAccessException if the constructor cannot be accessed
   * @throws NoSuchMethodException if the constructor does not exist
   * @throws SecurityException if security violation has occurred
   * @throws InvocationTargetException if the constructor invocation results in an exception
   */
  public static <T> T createNewClassInstance(Class<T> cls, Class<?>[] ctorClassArgs,
      Object[] ctorArgs) throws InstantiationException, IllegalAccessException,
      NoSuchMethodException, SecurityException, InvocationTargetException {
    if (ctorClassArgs == null) {
      return cls.newInstance();
    }
    Constructor<T> ctor = cls.getConstructor(ctorClassArgs);
    return ctor.newInstance(ctorArgs);
  }

  /**
   * Gets the current user's group list from Unix by running the command 'groups' NOTE. For
   * non-existing user it will return EMPTY list. This method may return duplicate groups.
   *
   * @param user user name
   * @return the groups list that the {@code user} belongs to. The primary group is returned first
   * @throws IOException if encounter any error when running the command
   */
  public static List<String> getUnixGroups(String user) throws IOException {
    String result = "";
    List<String> groups = Lists.newArrayList();
    try {
      result = ShellUtils.execCommand(ShellUtils.getGroupsForUserCommand(user));
    } catch (ExitCodeException e) {
      // if we didn't get the group - just return empty list;
      LOG.warn("got exception trying to get groups for user " + user + ": " + e.getMessage());
      return groups;
    }

    StringTokenizer tokenizer = new StringTokenizer(result, ShellUtils.TOKEN_SEPARATOR_REGEX);
    while (tokenizer.hasMoreTokens()) {
      groups.add(tokenizer.nextToken());
    }
    return groups;
  }

  /**
   * Using {@link GroupMappingService} to get the primary group name.
   *
   * @param conf the runtime configuration of Alluxio
   * @param userName Alluxio user name
   * @return primary group name
   * @throws IOException if getting group failed
   */
  public static String getPrimaryGroupName(Configuration conf, String userName) throws IOException {
    GroupMappingService groupMappingService =
        GroupMappingService.Factory.getUserToGroupsMappingService(conf);
    List<String> groups = groupMappingService.getGroups(userName);
    return (groups != null && groups.size() > 0) ? groups.get(0) : "";
  }

  private CommonUtils() {} // prevent instantiation
}
