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

package alluxio.cli;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.RuntimeConstants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FormatUtils;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Mini benchmark that writes/reads one file with a given size and operation type.
 */
@ThreadSafe
public final class MiniBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(MiniBenchmark.class);

  /**
   * The operation types to test.
   */
  enum OperationType {
    /**
     * Read from a file.
     */
    READ,
    /**
     * Write to a file.
     */
    WRITE,
  }

  private MiniBenchmark() {} // prevent instantiation

  /**
   * Prints the usage.
   */
  private static void usage() {
    new HelpFormatter().printHelp(String.format(
        "java -cp %s %s -type <[READ, WRITE]> -fileSize <fileSize> -iterations <iterations> "
            + "-concurrency <concurrency>",
        RuntimeConstants.ALLUXIO_JAR, MiniBenchmark.class.getCanonicalName()),
        "run a mini benchmark to write or read a file",
        OPTIONS, "", true);
  }

  private static final Options OPTIONS =
      new Options().addOption("help", false, "Show help for this test.")
          .addOption("type", true, "The operation type (either READ or WRITE).")
          .addOption("fileSize", true, "The file size (e.g. 1GB).")
          .addOption("iterations", true, "The number of iterations to run.")
          .addOption("concurrency", true, "The number of concurrent readers or writers.");

  private static boolean sHelp;
  private static OperationType sType;
  private static long sFileSize;
  private static int sIterations;
  private static int sConcurrency;

  /**
   * Parses the input args with a command line format, using
   * {@link org.apache.commons.cli.CommandLineParser}.
   *
   * @param args the input args
   * @return true if parsing succeeded
   */
  private static boolean parseInputArgs(String[] args) {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(OPTIONS, args);
    } catch (ParseException e) {
      System.out.println("Failed to parse input args: " + e);
      return false;
    }
    sHelp = cmd.hasOption("help");
    sType = OperationType.valueOf(cmd.getOptionValue("type", "READ"));
    sFileSize = FormatUtils.parseSpaceSize(cmd.getOptionValue("fileSize", "1KB"));
    sIterations = Integer.parseInt(cmd.getOptionValue("iterations", "1"));
    sConcurrency = Integer.parseInt(cmd.getOptionValue("concurrency", "1"));

    return true;
  }

  /**
   * @param args there are no arguments needed
   * @throws Exception if error occurs during tests
   */
  public static void main(String[] args) throws Exception {
    if (!parseInputArgs(args)) {
      usage();
      System.exit(-1);
    }
    if (sHelp) {
      usage();
      System.exit(0);
    }

    CommonUtils.warmUpLoop();
    AlluxioConfiguration alluxioConf = new InstancedConfiguration(ConfigurationUtils.defaults());

    for (int i = 0; i < sIterations; ++i) {
      final AtomicInteger count = new AtomicInteger(0);
      final CyclicBarrier barrier = new CyclicBarrier(sConcurrency);
      ExecutorService executorService = Executors.newFixedThreadPool(sConcurrency);
      final AtomicLong runtime = new AtomicLong(0);
      for (int j = 0; j < sConcurrency; ++j) {
        switch (sType) {
          case READ:
            executorService.submit(() -> {
              try {
                readFile(barrier, runtime, count.addAndGet(1), alluxioConf);
              } catch (Exception e) {
                LOG.error("Failed to read file.", e);
                System.exit(-1);
              }
            });
            break;
          case WRITE:
            executorService.submit(() -> {
              try {
                writeFile(barrier, runtime, count.addAndGet(1), alluxioConf);
              } catch (Exception e) {
                LOG.error("Failed to write file.", e);
                System.exit(-1);
              }
            });
            break;
          default:
            throw new RuntimeException("Unsupported type.");
        }
      }
      executorService.shutdown();
      Preconditions.checkState(executorService.awaitTermination(1, TimeUnit.HOURS));
      double time = runtime.get() * 1.0 / sConcurrency / Constants.SECOND_NANO;
      System.out
          .printf("Iteration: %d; Duration: %f seconds; Aggregated throughput: %f GB/second.%n", i,
              time, sConcurrency * 1.0 * sFileSize / time / Constants.GB);
    }
  }

  /**
   * Reads a file.
   *
   * @param count the count to determine the filename
   * @throws Exception if it fails to read
   */
  private static void readFile(CyclicBarrier barrier, AtomicLong runTime, int count,
      AlluxioConfiguration alluxioConf)
      throws Exception {
    FileSystem fileSystem = FileSystem.Factory.create(alluxioConf);
    byte[] buffer = new byte[(int) Math.min(sFileSize, 4 * Constants.MB)];

    barrier.await();
    long startTime = System.nanoTime();
    try (FileInStream inStream = fileSystem.openFile(filename(count))) {
      while (inStream.read(buffer) != -1) {
      }
    }
    runTime.addAndGet(System.nanoTime() - startTime);
  }

  /**
   * Writes a file.
   *
   * @param count the count to determine the filename
   * @throws Exception if it fails to write
   */
  private static void writeFile(CyclicBarrier barrier, AtomicLong runtime, int count,
      AlluxioConfiguration alluxioConf)
      throws Exception {
    FileSystem fileSystem = FileSystem.Factory.create(alluxioConf);
    byte[] buffer = new byte[(int) Math.min(sFileSize, 4 * Constants.MB)];
    Arrays.fill(buffer, (byte) 'a');
    AlluxioURI path = filename(count);

    if (fileSystem.exists(path)) {
      fileSystem.delete(path);
    }

    barrier.await();
    long startTime = System.nanoTime();
    long bytesWritten = 0;
    try (FileOutStream outStream = fileSystem.createFile(path)) {
      while (bytesWritten < sFileSize) {
        outStream.write(buffer, 0, (int) Math.min(buffer.length, sFileSize - bytesWritten));
        bytesWritten += buffer.length;
      }
    }
    runtime.addAndGet(System.nanoTime() - startTime);
  }

  private static AlluxioURI filename(int count) {
    return new AlluxioURI("/default_mini_benchmark_" + count);
  }
}
