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
import alluxio.client.WriteType;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.dora.WorkerLocationPolicy;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * Example to show the basic operations of Alluxio.
 */
public final class CheckCluster {
  // Constants should be all uppercase with underscores
  private static final AlluxioURI DIRECTORY = new AlluxioURI(AlluxioURI.SEPARATOR);
  private static final String TEST_CONTENT = "Hello Alluxio!";
  private static final int NUM_FILES = 1000;
  public static final String ANSI_RESET = "\u001B[0m";
  public static final String ANSI_RED = "\u001B[31m";
  public static final String ANSI_GREEN = "\u001B[32m";

  private CheckCluster() {
  } // Prevent instantiation

  /**
   * Main function to test the cluster setup.
   *
   * @param args command line arguments (none expected)
   */
  public static void main(String[] args) {
    setupConfiguration();

    try (FileSystemContext fsContext = FileSystemContext.create(Configuration.global())) {
      FileSystem fs = FileSystem.Factory.create(fsContext);
      List<BlockWorkerInfo> workers = fsContext.getCachedWorkers();

      // Test with caching
      AlluxioURI testDir = initializeTestDirectory(fs);
      HashMap<BlockWorkerInfo, AlluxioURI> workerMap = assignToWorkers(fs, testDir, workers, false);
      verifyFileAssignments(fs, workerMap, false);

      // Test without caching (only UFS)
      testDir = initializeTestDirectory(fs);
      workerMap = assignToWorkers(fs, testDir, workers, true);
      verifyFileAssignments(fs, workerMap, true);
    } catch (Exception e) {
      System.out.println("Exception: " + e.getMessage());
    }
  }

  private static void setupConfiguration() {
    Configuration.set(PropertyKey.USER_WORKER_SELECTION_POLICY,
        "alluxio.client.file.dora.ConsistentHashPolicy");
    Configuration.set(PropertyKey.DORA_CLIENT_UFS_FALLBACK_ENABLED, false);
  }

  private static AlluxioURI initializeTestDirectory(FileSystem fs) throws Exception {
    String testDirName = Random.class.getSimpleName() + System.currentTimeMillis();
    AlluxioURI testDir = new AlluxioURI(DIRECTORY + testDirName);
    if (!fs.exists(testDir)) {
      fs.createDirectory(testDir);
    }
    return testDir;
  }

  private static HashMap<BlockWorkerInfo, AlluxioURI> assignToWorkers(FileSystem fs,
                                                                      AlluxioURI testDir,
                                                                      List<BlockWorkerInfo> workers,
                                                                      boolean throughOnly)
      throws Exception {
    HashMap<BlockWorkerInfo, AlluxioURI> workerMap = new HashMap<>();
    WorkerLocationPolicy policy = WorkerLocationPolicy.Factory.create(Configuration.global());

    for (int i = 0; i < NUM_FILES && workerMap.size() < workers.size(); i++) {
      AlluxioURI testFile = new AlluxioURI(testDir.getPath() + "/" + i);
      writeFile(fs, testFile, throughOnly);
      List<BlockWorkerInfo> assignedWorkers =
          policy.getPreferredWorkers(workers, testFile.getPath(), 1);
      if (assignedWorkers.size() != 1 || workerMap.containsKey(assignedWorkers.get(0))) {
        fs.delete(testFile, DeletePOptions.newBuilder().setRecursive(true).build());
      } else {
        workerMap.put(assignedWorkers.get(0), testFile);
      }
    }

    if (workerMap.size() != workers.size()) {
      throw new Exception("Failed to assign workers for all files");
    }

    return workerMap;
  }

  private static void writeFile(FileSystem fs, AlluxioURI testFile, boolean throughOnly)
      throws Exception {
    WriteType writeType = throughOnly ? WriteType.THROUGH : WriteType.CACHE_THROUGH;
    try (FileOutStream outStream = fs.createFile(testFile,
        CreateFilePOptions.newBuilder().setWriteType(writeType.toProto()).build())) {
      outStream.write(TEST_CONTENT.getBytes());
    }
  }

  private static void verifyFileAssignments(FileSystem fs,
                                            HashMap<BlockWorkerInfo, AlluxioURI> workerMap,
                                            boolean throughOnly) throws Exception {
    int failedWorkers = 0;
    int successfulWorkers = 0;
    List<BlockWorkerInfo> failedWorkerList = new ArrayList<>();

    for (BlockWorkerInfo worker : workerMap.keySet()) {
      AlluxioURI testFile = workerMap.get(worker);
      URIStatus fileInfo = fs.getStatus(testFile);
      boolean isFailure = false;

      if (throughOnly && fileInfo.getInAlluxioPercentage() != 0) {
        isFailure = true;
      } else if (!throughOnly && fileInfo.getInAlluxioPercentage() != 100) {
        isFailure = true;
      }

      if (isFailure) {
        failedWorkers++;
        failedWorkerList.add(worker);
        System.out.println(ANSI_RED + "Failure for worker: " + worker + ANSI_RESET);
      } else {
        successfulWorkers++;
        System.out.println(ANSI_GREEN + "Success for worker: " + worker + ANSI_RESET);
      }
    }

    // Summarize the results
    System.out.println("\nTotal workers: " + workerMap.size());
    System.out.println(ANSI_GREEN + "Successful workers: " + successfulWorkers + ANSI_RESET);
    System.out.println(ANSI_RED + "Failed workers: " + failedWorkers + ANSI_RESET);

    if (!failedWorkerList.isEmpty()) {
      System.out.println(ANSI_RED + "\nList of failed workers:" + ANSI_RESET);
      for (BlockWorkerInfo failedWorker : failedWorkerList) {
        System.out.println(ANSI_RED + failedWorker.toString() + ANSI_RESET);
      }
    }
  }

  private static void verifyFileContent(FileSystem fs, AlluxioURI testFile) throws Exception {
    try (FileInStream inStream = fs.openFile(testFile)) {
      byte[] buf = new byte[TEST_CONTENT.length()];
      int bytesRead = inStream.read(buf);
      String readContent = new String(buf, 0, bytesRead);

      if (!TEST_CONTENT.equals(readContent)) {
        throw new Exception("Read content does not match written content!");
      } else {
        System.out.println("Read and write tests passed!");
      }
    }
  }
}
