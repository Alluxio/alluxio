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

import alluxio.cli.profiler.ProfilerClient;
import alluxio.util.FormatUtils;
import alluxio.util.JvmHeapDumper;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * Class to help profile clients with heapdumps. Combine with flamegraphs for full exploitabillty
 */
public class ClientProfiler {

  private static final String DEFAULT_DIR = "/alluxio-profiling";

  /**
   * The client type to user for profiling.
   *
   * - abstractfs is the alluxio hadoop client wrapper
   * - alluxio is the native alluxio java client
   * - hadoop is the java HDFS client
   */
  public enum ClientType {
    abstractfs,
    alluxio,
    hadoop,
  }

  /**
   * The operation type.
   *
   * - cleanup is a "fast" cleanup of all directories/files
   * - createFiles creates lots of files
   * - deleteFiles deletes lots of files
   * - listFiles lists lots of files
   * - readFiles reads lots of files
   */
  enum OperationType {
    cleanup,
    createFiles,
    deleteFiles,
    listFiles,
    readFiles,
  }

  @Parameter(names = {"-c", "--client"},
      description = "The type of client to profile.",
      required = true)
  private ClientType mClientType;

  @Parameter(names = {"-op", "--operation"},
      description = "The type of operation to perform on the filesystem.",
      required = true)
  private OperationType mOperation = OperationType.createFiles;

  /** The total number of files to create in the filesystem. */
  @Parameter(names = {"-n", "--num-files"}, description = "total number of files to operate on in"
      + " the filesystem.")
  private int mNumFiles = 5000;

  /** The total amount of data to write across all files in the filesystem. */
  @Parameter(names = {"-s", "--data-size"}, description = "Amount of data to write to use when "
      + "profiling.")
  private String mDataParam = "128m";
  private long mDataSize;

  /** The number of threads performing concurrent client operations. */
  @Parameter(names = {"-t", "--threads"},
      description = "total threads to perform operations concurrently")
  private int mNumThreads = 1;

  /** The number of client operations allowed per second. The amount executed will always be <= */
  @Parameter(names = {"-r", "--rate"}, description = "The max number of operations per second. -1"
      + " for unlimited rate")
  private int mOperationRate = -1;

  /** The base directory to store files. */
  @Parameter(names = {"-d", "--directory"}, description = "Base directory to store files")
  private String mDataDir = DEFAULT_DIR;

  @Parameter(names = {"--dump-interval"}, description = "Interval at which to collect heap dumps")
  private String mDumpInterval = null;

  @Parameter(names = {"--dry"}, description = "Perform a dry run by simply printing all operations")
  private boolean mDryRun = false;

  @Parameter(names = {"--files-per-dir"}, description = "number of leaf files that are allowed in"
      + " a directory")
  private int mFilesPerDir = 50;

  /**
   * New client profiler.
   * @param args command line arguments
   */
  public ClientProfiler(String[] args) {
    parseArgs(args);
  }

  /**
   * Profiles; creating heap dumps.
   */
  public void profile() throws InterruptedException {
    ProfilerClient.sDryRun = mDryRun;
    ProfilerClient client = ProfilerClient.Factory.create(mClientType);
    if (mOperationRate == -1) {
      mOperationRate = Integer.MAX_VALUE;
    }
    boolean heapDumpEnabled = mDumpInterval != null;
    JvmHeapDumper dumper = null;
    if (heapDumpEnabled) {
      dumper = new JvmHeapDumper(FormatUtils.parseTimeSize(mDumpInterval), "dumps",
          "dump-" + mClientType.toString());
      dumper.start();
    }
    long startTime = System.currentTimeMillis();
    try {
      switch (mOperation) {
        case createFiles:
          client.createFiles(mDataDir, mNumFiles, mFilesPerDir, mDataSize / mNumFiles, mNumThreads,
              mOperationRate);
          break;
        case deleteFiles:
          client.deleteFiles(mDataDir, mNumFiles, mFilesPerDir, mNumThreads, mOperationRate);
          break;
        case listFiles:
          client.listFiles(mDataDir, mNumFiles, mFilesPerDir, mNumThreads, mOperationRate);
          break;
        case readFiles:
          client.readFiles(mDataDir, mNumFiles, mFilesPerDir, mNumThreads, mOperationRate);
          break;
        case cleanup:
          client.cleanup(mDataDir);
          break;
        default:
          throw new IllegalArgumentException(String.format("%s is an invalid operationType",
              mOperation));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    long endTime = System.currentTimeMillis();

    if (heapDumpEnabled) {
      dumper.stopDumps();
    }
    long totalTimeMillis = endTime - startTime;
    System.out.println(
        String.format("Profiler for client %s took %d ms to finish", mClientType,
            totalTimeMillis));
  }

  private int parseArgs(String[] args) {
    JCommander jc = new JCommander(this);
    jc.setProgramName("Client Profiler");
    try {
      jc.parse(args);
      mDataSize = FormatUtils.parseSpaceSize(mDataParam);
    } catch (Exception e) {
      System.out.println(e.toString());
      jc.usage();
      System.exit(1);
    }
    return 0;
  }

  /**
   * Run main.
   * @param args arg
   */
  public static void main(String[] args) {
    ClientProfiler profiler = new ClientProfiler(args);
    try {
      profiler.profile();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
