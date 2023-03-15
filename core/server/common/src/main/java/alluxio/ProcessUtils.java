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

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

import alluxio.exception.status.DeadlineExceededException;
import alluxio.metrics.MetricsSystem;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadUtils;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static alluxio.metrics.sink.MetricsServlet.OBJECT_MAPPER;

/**
 * Utility methods for Alluxio {@link Process}es.
 */
public final class ProcessUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessUtils.class);

  public static final Set<CommonUtils.ProcessType> COLLECT_ON_EXIT =
      ImmutableSet.of(CommonUtils.ProcessType.MASTER, CommonUtils.ProcessType.WORKER);
  public static final AtomicBoolean METRIC_DUMP_CHECK = new AtomicBoolean(false);
  public static final AtomicBoolean STACK_DUMP_CHECK = new AtomicBoolean(false);
  public static final DateTimeFormatter DATETIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd-hhmmss");

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

  public static void dumpInformationOnExit() {
    dumpInformation(false);
  }

  public static void dumpInformationOnFailover() {
    dumpInformation(true);
  }

  private static void dumpInformation(boolean isFailover) {
    if (!COLLECT_ON_EXIT.contains(CommonUtils.PROCESS_TYPE.get())) {
      LOG.info("Process type is {}, skip dumping metrics and thread stacks", CommonUtils.PROCESS_TYPE.get());
      return;
    }

    LOG.info("Logging all useful information before exiting {}", CommonUtils.PROCESS_TYPE.get());

    if (!STACK_DUMP_CHECK.get()) {
      synchronized (ProcessUtils.class) {
        // Only attempt to dump the stacks once because it produces a lot of logs
        if (STACK_DUMP_CHECK.compareAndSet(false, true)) {
          if (isFailover) {
            if (Configuration.getBoolean(PropertyKey.MASTER_FAILOVER_COLLECT_STACKS)) {
              LOG.info("Logging all thread stacks when primary master switches to standby...");
              dumpStacks();
            } else {
              LOG.info("Not logging thread stacks on failover, set {}=true if that is necessary",
                  PropertyKey.MASTER_FAILOVER_COLLECT_STACKS.getName());
            }
          } else {
            if (Configuration.getBoolean(PropertyKey.EXIT_COLLECT_STACKS)) {
              LOG.info("Logging all thread stacks on exit...");
              dumpStacks();
            } else {
              LOG.info("Not logging thread stacks on exit, set {}=true if that is necessary",
                  PropertyKey.EXIT_COLLECT_STACKS.getName());
            }
          }
        }
      }
    }

    if (!METRIC_DUMP_CHECK.get()) {
      synchronized (ProcessUtils.class) {
        // Only attempt to dump threads once because it produces a lot of logs
        if (METRIC_DUMP_CHECK.compareAndSet(false, true)) {
          if (isFailover) {
            if (Configuration.getBoolean(PropertyKey.MASTER_FAILOVER_COLLECT_STACKS)) {
              LOG.info("Logging all metrics when primary master switches to standby...");
              dumpMetrics();
            } else {
              LOG.info("Not logging primary master metrics on failover, set {}=true if that is necessary",
                  PropertyKey.MASTER_FAILOVER_COLLECT_METRICS.getName());
            }
          } else {
            if (Configuration.getBoolean(PropertyKey.EXIT_COLLECT_METRICS)) {
              LOG.info("Logging all component metrics...");
              dumpMetrics();
            } else {
              LOG.info("Not logging component metrics on exit, set {}=true if that is necessary",
                  PropertyKey.EXIT_COLLECT_METRICS.getName());
            }
          }
        }
      }
    }
  }

  private static void dumpMetrics() {
    String logsPath = Configuration.getString(PropertyKey.LOGS_DIR);
    Instant start = Instant.now();
    String childFilePath = String.format("alluxio-%s-metrics-%s.json",
        CommonUtils.PROCESS_TYPE.get().toString().toLowerCase(), DATETIME_FORMAT.format(start));
    File metricDumpFile = new File(logsPath, childFilePath);
    try (FileOutputStream fos = new FileOutputStream(metricDumpFile, false)) {
      // The metrics json string is ~100KB in size
      String outputContents = OBJECT_MAPPER.writerWithDefaultPrettyPrinter()
          .writeValueAsString(MetricsSystem.METRIC_REGISTRY);
      fos.getChannel().write(ByteBuffer.wrap(outputContents.getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      LOG.error("Failed to persist metrics to {}", metricDumpFile.getAbsolutePath(), e);
    }
    Instant end = Instant.now();
    LOG.info("Dumped metrics of current process in {}ms to {}",
        Duration.between(start, end).toMillis(), childFilePath);
  }

  private static void dumpStacks() {
    String logsPath = Configuration.getString(PropertyKey.LOGS_DIR);
    Instant start = Instant.now();
    String childFilePath = String.format("alluxio-%s-stacks-%s.txt",
        CommonUtils.PROCESS_TYPE.get().toString().toLowerCase(), DATETIME_FORMAT.format(start));
    File stacksDumpFile = new File(logsPath, childFilePath);
    try (PrintStream stream = new PrintStream(stacksDumpFile)) {
      // Dumping one thread produces <1KB
      ThreadUtils.printThreadInfo(stream, "Dumping all threads in process");
    } catch (IOException e) {
      LOG.error("Failed to persist thread stacks to {}", stacksDumpFile.getAbsolutePath(), e);
    }
    Instant end = Instant.now();
    LOG.info("Dumped jstack of current process in {}ms to {}",
        Duration.between(start, end).toMillis(), childFilePath);
  }

  private ProcessUtils() {} // prevent instantiation
}
