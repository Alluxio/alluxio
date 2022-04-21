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

import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.proto.dataserver.Protocol;
import alluxio.security.group.CachedGroupMapping;
import alluxio.security.group.GroupMappingService;
import alluxio.util.ShellUtils.ExitCodeException;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.proto.ProtoUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.io.Closer;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.netty.channel.Channel;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
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

  private static final int JAVA_MAJOR_VERSION =
      parseMajorVersion(System.getProperty("java.version"));

  /**
   * Convenience method for calling {@link #createProgressThread(long, PrintStream)} with an
   * interval of 2 seconds.
   *
   * @param stream the print stream to write to
   * @return the thread
   */
  public static Thread createProgressThread(PrintStream stream) {
    return createProgressThread(2L * Constants.SECOND_MS, stream);
  }

  /**
   * Creates a thread which will write "." to the given print stream at the given interval. The
   * created thread is not started by this method. The created thread will be a daemon thread
   * and will halt when interrupted.
   *
   * @param intervalMs the time interval in milliseconds between writes
   * @param stream the print stream to write to
   * @return the thread
   */
  public static Thread createProgressThread(final long intervalMs, final PrintStream stream) {
    Thread t = new Thread(() -> {
      while (true) {
        CommonUtils.sleepMs(intervalMs);
        if (Thread.interrupted()) {
          return;
        }
        stream.print(".");
      }
    });
    t.setDaemon(true);
    return t;
  }

  /**
   * @return current time in milliseconds
   */
  public static long getCurrentMs() {
    return Instant.now().toEpochMilli();
  }

  /**
   * @return the current jvm major version, 8 for java 1.8 or 11 for java 11
   */
  public static int getJavaVersion() {
    return JAVA_MAJOR_VERSION;
  }

  /**
   * @param tmpDirs the list of possible temporary directories to pick from
   * @return a path to a temporary directory based on the user configuration
   */
  public static String getTmpDir(List<String> tmpDirs) {
    Preconditions.checkState(!tmpDirs.isEmpty(), "No temporary directories available");
    if (tmpDirs.size() == 1) {
      return tmpDirs.get(0);
    }
    // Use existing random instead of ThreadLocal because contention is not expected to be high.
    return tmpDirs.get(RANDOM.nextInt(tmpDirs.size()));
  }

  /**
   * @param storageDir the root of a storage directory in tiered storage
   * @param conf Alluxio's current configuration
   *
   * @return the worker data folder path after each storage directory, the final path will be like
   * "/mnt/ramdisk/alluxioworker" for storage dir "/mnt/ramdisk" by appending
   * {@link PropertyKey#WORKER_DATA_FOLDER}.
   */
  public static String getWorkerDataDirectory(String storageDir, AlluxioConfiguration conf) {
    return PathUtils.concatPath(
        storageDir.trim(), conf.get(PropertyKey.WORKER_DATA_FOLDER));
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
    // TODO(adit): remove this wrapper
    SleepUtils.sleepMs(timeMs);
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
    // TODO(adit): remove this wrapper
    SleepUtils.sleepMs(logger, timeMs);
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
   * @return new class object
   * @throws RuntimeException if the class cannot be instantiated
   */
  public static <T> T createNewClassInstance(Class<T> cls, Class<?>[] ctorClassArgs,
      Object[] ctorArgs) {
    try {
      if (ctorClassArgs == null) {
        return cls.newInstance();
      }
      Constructor<T> ctor = cls.getConstructor(ctorClassArgs);
      return ctor.newInstance(ctorArgs);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e.getCause());
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the current user's group list from Unix by running the command 'groups' NOTE. For
   * non-existing user it will return EMPTY list. This method may return duplicate groups.
   *
   * @param user user name
   * @return the groups list that the {@code user} belongs to. The primary group is returned first
   */
  public static List<String> getUnixGroups(String user) throws IOException {
    String result;
    List<String> groups = new ArrayList<>();
    try {
      result = ShellUtils.execCommand(ShellUtils.getGroupsForUserCommand(user));
    } catch (ExitCodeException e) {
      // if we didn't get the group - just return empty list
      LOG.warn("got exception trying to get groups for user {}: {}", user, e.toString());
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
   * @param description a description of what causes condition to evaluate to true
   * @param condition the condition to wait on
   * @throws TimeoutException if the function times out while waiting for the condition to be true
   */
  public static void waitFor(String description, Supplier<Boolean> condition)
      throws InterruptedException, TimeoutException {
    waitFor(description, condition, WaitForOptions.defaults());
  }

  /**
   * Waits for a condition to be satisfied.
   *
   * @param description a description of what causes condition to evaluate to true
   * @param condition the condition to wait on
   * @param options the options to use
   * @throws TimeoutException if the function times out while waiting for the condition to be true
   */
  public static void waitFor(String description, Supplier<Boolean> condition,
      WaitForOptions options) throws InterruptedException, TimeoutException {
    waitForResult(description, condition, (b) -> b, options);
  }

  /**
   * Waits for the object to meet a certain condition.
   *
   * @param description a description of what causes condition to be met
   * @param objectSupplier the object to check the condition for
   * @param condition the condition to wait on
   * @param <T> type of the object
   * @return the object
   * @throws TimeoutException if the function times out while waiting for the condition to be true
   * @throws InterruptedException if the thread was interrupted
   */
  public static <T> T waitForResult(String description, Supplier<T> objectSupplier,
                                    Function<T, Boolean> condition)
      throws TimeoutException, InterruptedException {
    return waitForResult(description, objectSupplier, condition, WaitForOptions.defaults());
  }

  /**
   * Waits for the object to meet a certain condition.
   *
   * @param description a description of what causes condition to be met
   * @param objectSupplier the object to check the condition for
   * @param condition the condition to wait on
   * @param options the options to use
   * @param <T> type of the object
   * @return the object
   * @throws TimeoutException if the function times out while waiting for the condition to be true
   * @throws InterruptedException if the thread was interrupted
   */
  public static <T> T waitForResult(String description, Supplier<T> objectSupplier,
                                    Function<T, Boolean> condition, WaitForOptions options)
      throws TimeoutException, InterruptedException {
    T value;
    long start = getCurrentMs();
    int interval = options.getInterval();
    int timeout = options.getTimeoutMs();
    while (condition.apply(value = objectSupplier.get()) != true) {
      if (timeout != WaitForOptions.NEVER && getCurrentMs() - start > timeout) {
        throw new TimeoutException("Timed out waiting for " + description + " options: " + options
            + " last value: " + ObjectUtils.toString(value));
      }
      Thread.sleep(interval);
    }
    return value;
  }

  /**
   * Gets the primary group name of a user.
   *
   * @param userName Alluxio user name
   * @param conf Alluxio configuration
   * @return primary group name
   */
  public static String getPrimaryGroupName(String userName, AlluxioConfiguration conf)
      throws IOException {
    List<String> groups = getGroups(userName, conf);
    return (groups != null && groups.size() > 0) ? groups.get(0) : "";
  }

  /**
   * Using {@link CachedGroupMapping} to get the group list of a user.
   *
   * @param userName Alluxio user name
   * @param conf Alluxio configuration
   * @return the group list of the user
   */
  public static List<String> getGroups(String userName, AlluxioConfiguration conf)
      throws IOException {
    GroupMappingService groupMappingService = GroupMappingService.Factory.get(conf);
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
   * Strips the leading and trailing quotes from the given string.
   * E.g. return 'alluxio' for input '"alluxio"'.
   *
   * @param str The string to strip
   * @return The string without the leading and trailing quotes
   */
  public static String stripLeadingAndTrailingQuotes(String str) {
    int length = str.length();
    if (length > 1 && str.startsWith("\"") && str.endsWith("\"")) {
      str = str.substring(1, length - 1);
    }
    return str;
  }

  /**
   * Gets the value with a given key from a static key/value mapping in string format. E.g. with
   * mapping "id1=user1;id2=user2", it returns "user1" with key "id1". It returns null if the given
   * key does not exist in the mapping.
   *
   * @param mapping the "key=value" mapping in string format separated by ";"
   * @param key the key to query
   * @return the mapped value if the key exists, otherwise returns null
   */
  @Nullable
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
   * It stops at encountering gRPC's StatusRuntimeException.
   *
   * @param e the exception
   * @return the root cause
   */
  public static Throwable getRootCause(Throwable e) {
    while (e.getCause() != null && !(e.getCause() instanceof StatusRuntimeException)) {
      e = e.getCause();
    }
    return e;
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

  /**
   * Returns an iterator that iterates on a single element.
   *
   * @param element the element
   * @param <T> the type of the element
   * @return the iterator
   */
  public static <T> Iterator<T> singleElementIterator(final T element) {
    return new Iterator<T>() {
      private boolean mHasNext = true;

      @Override
      public boolean hasNext() {
        return mHasNext;
      }

      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        mHasNext = false;
        return element;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove is not supported.");
      }
    };
  }

  /**
   * Executes the given callables, waiting for them to complete (or time out). If a callable throws
   * an exception, that exception will be re-thrown from this method.
   *
   * @param callables the callables to execute
   * @param timeoutMs time to wait for the callables to complete, in milliseconds
   * @param <T> the return type of the callables
   * @throws TimeoutException if the callables don't complete before the timeout
   * @throws ExecutionException if any of the callables throws an exception
   */
  public static <T> void invokeAll(List<Callable<T>> callables, long timeoutMs)
      throws TimeoutException, ExecutionException {
    ExecutorService service = Executors.newCachedThreadPool();
    try {
      invokeAll(service, callables, timeoutMs);
    } finally {
      service.shutdownNow();
    }
  }

  /**
   * Executes the given callables, waiting for them to complete (or time out). If a callable throws
   * an exception, that exception will be re-thrown from this method. If the current thread is
   * interrupted, an Exception will be thrown.
   *
   * @param service the service to execute the callables
   * @param callables the callables to execute
   * @param timeoutMs time to wait for the callables to complete, in milliseconds
   * @param <T> the return type of the callables
   * @throws TimeoutException if the callables don't complete before the timeout
   * @throws ExecutionException if any of the callables throws an exception
   */
  public static <T> void invokeAll(ExecutorService service, List<Callable<T>> callables,
      long timeoutMs) throws TimeoutException, ExecutionException {
    long endMs = getCurrentMs() + timeoutMs;
    List<Future<T>> pending = new ArrayList<>();
    for (Callable<T> c : callables) {
      pending.add(service.submit(c));
    }
    // Poll the tasks to exit early in case of failure.
    while (!pending.isEmpty()) {
      if (Thread.interrupted()) {
        // stop waiting if interrupted
        Thread.currentThread().interrupt();
        throw new RuntimeException("Thread was interrupted while waiting for invokeAll.");
      }
      Iterator<Future<T>> it = pending.iterator();
      while (it.hasNext()) {
        Future<T> future = it.next();
        if (future.isDone()) {
          // Check whether the callable threw an exception.
          try {
            future.get();
          } catch (InterruptedException e) {
            // This should never happen since we already checked isDone().
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
          it.remove();
        }
      }
      if (pending.isEmpty()) {
        break;
      }
      long remainingMs = endMs - getCurrentMs();
      if (remainingMs <= 0) {
        // Cancel the pending futures
        for (Future<T> future : pending) {
          future.cancel(true);
        }
        throw new TimeoutException(
            String.format("Timed out after %dms", timeoutMs - remainingMs));
      }
      CommonUtils.sleepMs(Math.min(remainingMs, 50));
    }
  }

  /**
   * Closes the Closer and re-throws the Throwable. Any exceptions thrown while closing the Closer
   * will be added as suppressed exceptions to the Throwable. This method always throws the given
   * Throwable, wrapping it in a RuntimeException if it's a non-IOException checked exception.
   *
   * Note: This method will wrap non-IOExceptions in RuntimeException. Do not use this method in
   * methods which throw non-IOExceptions.
   *
   * <pre>
   * Closer closer = new Closer();
   * try {
   *   Closeable c = closer.register(new Closeable());
   * } catch (Throwable t) {
   *   throw closeAndRethrow(closer, t);
   * }
   * </pre>
   *
   * @param closer the Closer to close
   * @param t the Throwable to re-throw
   * @return this method never returns
   */
  public static RuntimeException closeAndRethrow(Closer closer, Throwable t) throws IOException {
    try {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  /**
   * Similar to {@link CommonUtils#closeAndRethrow} but always return a RuntimeException.
   *
   * @param closer the Closer to close
   * @param t the Throwable to re-throw
   * @return this method never returns
   */
  public static RuntimeException closeAndRethrowRuntimeException(Closer closer, Throwable t) {
    try {
      throw closer.rethrow(t instanceof IOException ? new RuntimeException(t) : t);
    } catch (IOException e) {
      // we shall never reach here as no IOException enters rethrow
    } finally {
      try {
        closer.close();
      } catch (IOException e) {
        // we shall never reach here as close catches all throwable
      }
    }
    return new IllegalStateException("this method shall never return");
  }

  /** Alluxio process types. */
  public enum ProcessType {
    JOB_MASTER,
    JOB_WORKER,
    CLIENT,
    MASTER,
    PLUGIN,
    PROXY,
    WORKER;
  }

  /**
   * Represents the type of Alluxio process running in this JVM.
   *
   * NOTE: This will only be set by main methods of Alluxio processes. It will not be set properly
   * for tests. Avoid using this field if at all possible.
   */
  public static final AtomicReference<ProcessType> PROCESS_TYPE =
      new AtomicReference<>(ProcessType.CLIENT);

  /**
   * Unwraps a {@link alluxio.proto.dataserver.Protocol.Response}.
   *
   * @param response the response
   */
  public static void unwrapResponse(Protocol.Response response) throws AlluxioStatusException {
    Status status = ProtoUtils.fromProto(response.getStatus());
    if (status != Status.OK) {
      throw AlluxioStatusException.from(status.withDescription(response.getMessage()));
    }
  }

  /**
   * Unwraps a {@link alluxio.proto.dataserver.Protocol.Response} associated with a channel.
   *
   * @param response the response
   * @param channel the channel that receives this response
   */
  public static void unwrapResponseFrom(Protocol.Response response, Channel channel)
      throws AlluxioStatusException {
    Status status = ProtoUtils.fromProto(response.getStatus());
    if (status != Status.OK) {
      throw AlluxioStatusException.from(status.withDescription(
          String.format("Channel to %s: %s", channel.remoteAddress(), response.getMessage())));
    }
  }

  /**
   * @param address the Alluxio worker network address
   * @param conf Alluxio configuration
   * @return true if the worker is local
   */
  public static boolean isLocalHost(WorkerNetAddress address, AlluxioConfiguration conf) {
    return address.getHost().equals(NetworkAddressUtils.getClientHostName(conf));
  }

  /**
   * Converts a millisecond number to a formatted date String.
   *
   * @param millis a long millisecond number
   * @param dateFormatPattern the date format to follow when converting. i.e. mm-dd-yyyy
   * @return formatted date String
   */
  public static String convertMsToDate(long millis, String dateFormatPattern) {
    DateFormat dateFormat = new SimpleDateFormat(dateFormatPattern);
    return dateFormat.format(new Date(millis));
  }

  /**
   * Converts milliseconds to clock time.
   *
   * @param millis milliseconds
   * @return input encoded as clock time
   */
  public static String convertMsToClockTime(long millis) {
    Preconditions.checkArgument(millis >= 0,
        "Negative values %s are not supported to convert to clock time.", millis);

    long days = millis / Constants.DAY_MS;
    long hours = (millis % Constants.DAY_MS) / Constants.HOUR_MS;
    long mins = (millis % Constants.HOUR_MS) / Constants.MINUTE_MS;
    long secs = (millis % Constants.MINUTE_MS) / Constants.SECOND_MS;

    return String.format("%d day(s), %d hour(s), %d minute(s), and %d second(s)", days, hours,
        mins, secs);
  }

  /**
   * @param input the input map
   * @return a map using protobuf {@link ByteString} for values instead of {@code byte[]}
   */
  public static Map<String, ByteString> convertToByteString(Map<String, byte[]> input) {
    if (input == null) {
      return Collections.emptyMap();
    }
    Map<String, ByteString> output = new HashMap<>(input.size());
    input.forEach((k, v) -> output.put(k, ByteString.copyFrom(v)));
    return output;
  }

  /**
   * @param input the input map
   * @return a map using {@code byte[]} for values instead of protobuf {@link ByteString}
   */
  public static Map<String, byte[]> convertFromByteString(Map<String, ByteString> input) {
    if (input == null) {
      return Collections.emptyMap();
    }
    Map<String, byte[]> output = new HashMap<>(input.size());
    input.forEach((k, v) -> output.put(k, v.toByteArray()));
    return output;
  }

  /**
   * Memoize implementation for java.util.function.supplier.
   *
   * @param original the original supplier
   * @param <T> the object type
   * @return the supplier with memorization
   */
  public static <T> Supplier<T> memoize(Supplier<T> original) {
    return new Supplier<T>() {
      Supplier<T> mDelegate = this::firstTime;
      boolean mInitialized;
      public T get() {
        return mDelegate.get();
      }

      private synchronized T firstTime() {
        if (!mInitialized) {
          T value = original.get();
          mDelegate = () -> value;
          mInitialized = true;
        }
        return mDelegate.get();
      }
    };
  }

  /**
   * Partitions a list into numLists many lists each with around list.size() / numLists elements.
   *
   * @param list the list to partition
   * @param numLists number of lists to return
   * @param <T> the object type
   * @return partitioned list
   */
  public static <T> List<List<T>> partition(List<T> list, int numLists) {
    ArrayList<List<T>> result = new ArrayList<>(numLists);

    for (int i = 0; i < numLists; i++) {
      result.add(new ArrayList<>(list.size() / numLists + 1));
    }

    for (int i = 0; i < list.size(); i++) {
      result.get(i % numLists).add(list.get(i));
    }

    return result;
  }

  /**
   * Validates whether a network address is reachable.
   *
   * @param hostname host name of the network address
   * @param port port of the network address
   * @param timeoutMs duration to attempt connection before returning false
   * @return whether the network address is reachable
   */
  public static boolean isAddressReachable(String hostname, int port, int timeoutMs) {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(hostname, port), timeoutMs);
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Recursively lists a local dir and all its subdirs and return all the files.
   *
   * @param dir the directory
   * @return a list of all the files
   * */
  public static List<File> recursiveListLocalDir(File dir) {
    File[] files = dir.listFiles();
    // File#listFiles can return null when the path is invalid
    if (files == null) {
      return Collections.emptyList();
    }
    List<File> result = new ArrayList<>(files.length);
    for (File f : files) {
      if (f.isDirectory()) {
        result.addAll(recursiveListLocalDir(f));
        continue;
      }
      result.add(f);
    }
    return result;
  }

  /**
   * @param version the version string of the JVMgi
   * @return the major version of the current JVM, 8 for 1.8, 11 for java 11
   * see https://www.oracle.com/java/technologies/javase/versioning-naming.html for reference
   */
  public static int parseMajorVersion(String version) {
    if (version.startsWith("1.")) {
      version = version.substring(2, 3);
    } else {
      int dot = version.indexOf(".");
      if (dot != -1) {
        version = version.substring(0, dot);
      }
    }
    return Integer.parseInt(version);
  }

  /**
   * @param obj a Java object
   * @return if this object is an collection
   */
  public static boolean isCollection(Object obj) {
    return obj instanceof Collection || obj instanceof Map;
  }

  /**
   * @param obj a Java object
   * @return a string to summarize the object if this is a collection or a map
   */
  public static String summarizeCollection(Object obj) {
    if (obj instanceof Collection) {
      return String.format(
          "%s{%d entries}", obj.getClass().getSimpleName(), ((Collection<?>) obj).size());
    } else if (obj instanceof Map) {
      return String.format("Map{%d entries}", ((Map<?, ?>) obj).size());
    }
    return Objects.toString(obj);
  }

  private CommonUtils() {} // prevent instantiation
}
