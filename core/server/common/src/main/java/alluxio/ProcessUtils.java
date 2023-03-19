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

package alluxio;

import static alluxio.metrics.sink.MetricsServlet.OBJECT_MAPPER;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadUtils;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Utility methods for Alluxio {@link Process}es.
 */
public final class ProcessUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessUtils.class);

  public static final Set<CommonUtils.ProcessType> COLLECT_ON_EXIT =
      ImmutableSet.of(CommonUtils.ProcessType.MASTER, CommonUtils.ProcessType.WORKER);
  public static volatile boolean sInfoDumpOnExitCheck = false;
  public static final DateTimeFormatter DATETIME_FORMAT =
      DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT).ofPattern("yyyyMMdd-HHmmss")
          .withLocale(Locale.getDefault()).withZone(ZoneId.systemDefault());

  /**
   * Runs the given {@link Process}. This method should only be called from {@code main()} methods.
   *
   * @param process the {@link Process} to run
   */
  public static void run(Process process) {
    try {
      LOG.info("Starting {}.", process);
      LOG.info("Alluxio version: {}-{}", RuntimeConstants.VERSION, ProjectConstants.REVISION);
      LOG.info("Java version: {}", System.getProperty("java.version"));
      process.start();
      LOG.info("Stopping {}.", process);

      dumpInformationOnExit();

      System.exit(0);
    } catch (Throwable t) {
      LOG.error("Uncaught exception while running {}, stopping it and exiting. "
          + "Exception \"{}\", Root Cause \"{}\"", process, t, Throwables.getRootCause(t), t);
      try {
        process.stop();
      } catch (Throwable t2) {
        // continue to exit
        LOG.error("Uncaught exception while stopping {}, simply exiting. "
            + "Exception \"{}\", Root Cause \"{}\"", process, t2, Throwables.getRootCause(t2),
            t2);
      }
      dumpInformationOnExit();

      System.exit(-1);
    }
  }

  /**
   * Logs a fatal error and then exits the system.
   *
   * @param logger the logger to log to
   * @param format the error message format string
   * @param args args for the format string
   */
  public static void fatalError(Logger logger, String format, Object... args) {
    fatalError(logger, new Throwable(), format, args);
  }

  /**
   * Logs a fatal error and then exits the system.
   *
   * @param logger the logger to log to
   * @param t the throwable causing the fatal error
   * @param format the error message format string
   * @param args args for the format string
   */
  public static void fatalError(Logger logger, Throwable t, String format, Object... args) {
    String message = String.format("Fatal error: " + format, args);
    if (t != null) {
      message += "\n" + Throwables.getStackTraceAsString(t);
    }
    if (Configuration.getBoolean(PropertyKey.TEST_MODE)) {
      throw new RuntimeException(message);
    }
    logger.error(message);

    dumpInformationOnExit();

    System.exit(-1);
  }

  /**
   * Adds a shutdown hook that will be invoked when a signal is sent to this process.
   *
   * The process may be utilizing some resources, and this shutdown hook will be invoked by
   * JVM when a SIGTERM is sent to the process by "kill" command. The shutdown hook calls
   * {@link Process#stop()} method to cleanly release the resources and exit.
   *
   * @param process the data structure representing the process to terminate
   */
  public static void stopProcessOnShutdown(final Process process) {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        dumpInformationOnExit();
        process.stop();
      } catch (Throwable t) {
        LOG.error("Failed to stop process", t);
      }
    }, "alluxio-process-shutdown-hook"));
  }

  /**
   * Outputs process critical information like metrics and jstack before it exits.
   * This is synchronous in order to capture as much information at the scene as possible.
   * The information will be output to separate files in the log directory.
   */
  public static void dumpInformationOnExit() {
    if (!COLLECT_ON_EXIT.contains(CommonUtils.PROCESS_TYPE.get())) {
      LOG.info("Process type is {}, skip dumping metrics and thread stacks",
          CommonUtils.PROCESS_TYPE.get());
      return;
    }
    if (Configuration.getBoolean(PropertyKey.EXIT_COLLECT_INFO)) {
      synchronized (ProcessUtils.class) {
        if (!sInfoDumpOnExitCheck) {
          sInfoDumpOnExitCheck = true;
          LOG.info("Logging metrics and jstack on {} exit...", CommonUtils.PROCESS_TYPE.get());
          try {
            String logsDir = Configuration.getString(PropertyKey.LOGS_DIR);
            String outputFilePrefix = "alluxio-"
                + CommonUtils.PROCESS_TYPE.get().toString().toLowerCase() + "-exit";
            dumpMetrics(logsDir, outputFilePrefix);
            dumpStacks(logsDir, outputFilePrefix);
          } catch (Throwable t) {
            LOG.error("Failed to dump metrics and jstacks", t);
          }
        }
      }
    } else {
      LOG.info("Not logging metrics and jstack on exit, set {}=true to enable this feature",
          PropertyKey.EXIT_COLLECT_INFO.getName());
    }
  }

  /**
   * Outputs process critical information like metrics and jstack before the primary master
   * fails over to standby. This is asynchronous in order not to block the failover.
   * The information will be output to separate files in the log directory.
   *
   * @param es the thread pool to submit tasks to
   * @return a list of futures for async info dumping jobs
   */
  public static List<Future<Void>> dumpInformationOnFailover(ExecutorService es) {
    if (Configuration.getBoolean(PropertyKey.MASTER_FAILOVER_COLLECT_INFO)) {
      LOG.info("Logging metrics and jstack when primary master switches to standby...");
      String logsDir = Configuration.getString(PropertyKey.LOGS_DIR);
      String outputFilePrefix = "alluxio-"
          + CommonUtils.PROCESS_TYPE.get().toString().toLowerCase() + "-failover";
      List<Future<Void>> futures = new ArrayList<>();
      // Attempt to dump metrics first before MetricsMaster clears all metrics
      // The failover procedure will shutdown RPC -> Journal -> Master components
      // So we rely on the first two steps take longer than this thread
      futures.add(es.submit(() -> {
        ProcessUtils.dumpMetrics(logsDir, outputFilePrefix);
        return null;
      }));
      futures.add(es.submit(() -> {
        ProcessUtils.dumpStacks(logsDir, outputFilePrefix);
        return null;
      }));
      LOG.info("Started dumping metrics and jstacks into {}", logsDir);
      return futures;
    } else {
      LOG.info("Not logging information like metrics and jstack on failover, "
          + "set {}=true to enable this feature",
          PropertyKey.MASTER_FAILOVER_COLLECT_INFO.getName());
      return Collections.emptyList();
    }
  }

  private static void dumpMetrics(String logsDir, String outputFilePrefix) {
    Instant start = Instant.now();
    String childFilePath = String.format("%s-metrics-%s.json",
        outputFilePrefix, DATETIME_FORMAT.format(start));
    File metricDumpFile = new File(logsDir, childFilePath);
    try (FileOutputStream fos = new FileOutputStream(metricDumpFile, false)) {
      // The metrics json string is ~100KB in size
      String outputContents = OBJECT_MAPPER.writerWithDefaultPrettyPrinter()
          .writeValueAsString(MetricsSystem.METRIC_REGISTRY);
      fos.getChannel().write(ByteBuffer.wrap(outputContents.getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      LOG.error("Failed to persist metrics to {}", metricDumpFile.getAbsolutePath(), e);
      return;
    }
    Instant end = Instant.now();
    LOG.info("Dumped metrics of current process in {}ms to {}",
        Duration.between(start, end).toMillis(), childFilePath);
  }

  private static void dumpStacks(String logsDir, String outputFilePrefix) {
    Instant start = Instant.now();
    String childFilePath = String.format("%s-stacks-%s.txt",
        outputFilePrefix, DATETIME_FORMAT.format(start));
    File stacksDumpFile = new File(logsDir, childFilePath);
    try (PrintStream stream = new PrintStream(stacksDumpFile)) {
      // Dumping one thread produces <1KB
      ThreadUtils.printThreadInfo(stream, "Dumping all threads in process");
    } catch (IOException e) {
      LOG.error("Failed to persist thread stacks to {}", stacksDumpFile.getAbsolutePath(), e);
      return;
    }
    Instant end = Instant.now();
    LOG.info("Dumped jstack of current process in {}ms to {}",
        Duration.between(start, end).toMillis(), childFilePath);
  }

  private ProcessUtils() {} // prevent instantiation
}
