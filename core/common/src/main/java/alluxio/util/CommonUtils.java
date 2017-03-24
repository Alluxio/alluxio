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

import alluxio.security.group.CachedGroupMapping;
import alluxio.security.group.GroupMappingService;
import alluxio.util.ShellUtils.ExitCodeException;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Common utilities shared by all components in Alluxio.
 */
@ThreadSafe
public final class CommonUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CommonUtils.class);

  private static final String ALPHANUM =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
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
   * Generates a random alphanumeric string of the given length.
   *
   * @param length the length
   * @return a random string
   */
  public static String randomAlphaNumString(int length) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      sb.append(ALPHANUM.charAt(RANDOM.nextInt(ALPHANUM.length())));
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
   * Unlike Thread.sleep(), this method responds to interrupts by setting the thread interrupt
   * status. This means that callers must check the interrupt status if they need to handle
   * interrupts.
   *
   * @param logger logger for reporting interruptions; no reporting is done if the logger is null
   * @param timeMs sleep duration in milliseconds
   */
  public static void sleepMs(Logger logger, long timeMs) {
    try {
      Thread.sleep(timeMs);
    } catch (InterruptedException e) {
      if (logger != null) {
        logger.warn(e.getMessage(), e);
      }
      Thread.currentThread().interrupt();
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
    String result;
    List<String> groups = new ArrayList<>();
    try {
      result = ShellUtils.execCommand(ShellUtils.getGroupsForUserCommand(user));
    } catch (ExitCodeException e) {
      // if we didn't get the group - just return empty list
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
   * Waits for a condition to be satisfied.
   *
   * @param description a description of what causes condition to evaluation to true
   * @param condition the condition to wait on
   */
  public static void waitFor(String description, Function<Void, Boolean> condition) {
    waitFor(description, condition, WaitForOptions.defaults());
  }

  /**
   * Waits for a condition to be satisfied.
   *
   * @param description a description of what causes condition to evaluation to true
   * @param condition the condition to wait on
   * @param options the options to use
   */
  public static void waitFor(String description, Function<Void, Boolean> condition,
      WaitForOptions options) {
    long start = System.currentTimeMillis();
    int interval = options.getInterval();
    int timeout = options.getTimeout();
    while (!condition.apply(null)) {
      if (timeout != WaitForOptions.NEVER && System.currentTimeMillis() - start > timeout) {
        throw new RuntimeException("Timed out waiting for " + description);
      }
      CommonUtils.sleepMs(interval);
    }
  }

  /**
   * Waits for an operation to return a non-null value with a specified timeout.
   *
   * @param description the description of this operation
   * @param operation the operation
   * @param options the options to use
   * @param <T> the type of the return value
   * @return the return value, null if it times out
   */
  public static <T> T waitForResult(String description, Function<Void, T> operation,
      WaitForOptions options) {
    T t;
    long start = System.currentTimeMillis();
    int interval = options.getInterval();
    int timeout = options.getTimeout();
    while ((t = operation.apply(null)) == null) {
      if (timeout != WaitForOptions.NEVER && System.currentTimeMillis() - start > timeout) {
        throw new RuntimeException("Timed out waiting for " + description);
      }
      CommonUtils.sleepMs(interval);
    }
    return t;
  }

  /**
   * Gets the primary group name of a user.
   *
   * @param userName Alluxio user name
   * @return primary group name
   * @throws IOException if getting group failed
   */
  public static String getPrimaryGroupName(String userName) throws IOException {
    List<String> groups = getGroups(userName);
    return (groups != null && groups.size() > 0) ? groups.get(0) : "";
  }

  /**
   * Using {@link CachedGroupMapping} to get the group list of a user.
   *
   * @param userName Alluxio user name
   * @return the group list of the user
   * @throws IOException if getting group list failed
   */
  public static List<String> getGroups(String userName) throws IOException {
    GroupMappingService groupMappingService = GroupMappingService.Factory.get();
    return groupMappingService.getGroups(userName);
  }

  /**
   * Strips the suffix if it exists. This method will leave keys without a suffix unaltered.
   *
   * @param key the key to strip the suffix from
   * @param suffix suffix to remove
   * @return the key with the suffix removed, or the key unaltered if the suffix is not present
   */
  public static String stripSuffixIfPresent(final String key, final String suffix) {
    if (key.endsWith(suffix)) {
      return key.substring(0, key.length() - suffix.length());
    }
    return key;
  }

  /**
   * Strips the prefix from the key if it is present. For example, for input key
   * ufs://my-bucket-name/my-key/file and prefix ufs://my-bucket-name/, the output would be
   * my-key/file. This method will leave keys without a prefix unaltered, ie. my-key/file
   * returns my-key/file.
   *
   * @param key the key to strip
   * @param prefix prefix to remove
   * @return the key without the prefix
   */
  public static String stripPrefixIfPresent(final String key, final String prefix) {
    if (key.startsWith(prefix)) {
      return key.substring(prefix.length());
    }
    return key;
  }

  /**
   * Gets the value with a given key from a static key/value mapping in string format. E.g. with
   * mapping "id1=user1;id2=user2", it returns "user1" with key "id1". It returns null if the given
   * key does not exist in the mapping.
   *
   * @param mapping the "key=value" mapping in string format separated by ";"
   * @param key the key to query
   * @return the mapped value if the key exists, otherwise returns ""
   */
  public static String getValueFromStaticMapping(String mapping, String key) {
    Map<String, String> m = Splitter.on(";")
        .omitEmptyStrings()
        .trimResults()
        .withKeyValueSeparator("=")
        .split(mapping);
    return m.get(key);
  }

  /**
   * Gets the root cause of an exception.
   *
   * @param e the exception
   * @return the root cause
   */
  public static Throwable getRootCause(Throwable e) {
    while (e.getCause() != null) {
      e = e.getCause();
    }
    return e;
  }

  /**
   * Closes a closer and ignores the IOException if it throws one.
   *
   * @param closer the closer
   */
  public static void closeQuitely(Closer closer) {
    try {
      closer.close();
    } catch (IOException e) {
      // Ignore.
    }
  }

  /**
   * Casts a {@link Throwable} to an {@link IOException}.
   *
   * @param e the throwable
   * @return the IO exception
   */
  public static IOException castToIOException(Throwable e) {
    if (e instanceof IOException) {
      return (IOException) e;
    } else {
      return new IOException(e);
    }
  }

  private CommonUtils() {} // prevent instantiation
}
