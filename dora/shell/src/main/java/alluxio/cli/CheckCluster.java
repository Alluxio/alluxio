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
import alluxio.client.file.DoraCacheFileSystem;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.dora.WorkerLocationPolicy;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.membership.WorkerClusterView;
import alluxio.wire.WorkerInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * In this test, we cover all workers by performing the following steps:
 * 1. We initialize a test directory in Alluxio's distributed file system.
 * 2. We assign files to different block workers based on a policy (e.g., consistent hashing).
 * 3. We verify the assignments and content of the files.
 * 4. If a worker fails during the test, we add it to the list of FAILED_WORKER.
 * 5. At the end of the test, this method prints information about all the failed workers,
 *    including their hostname, port, available space, and capacity.
 * This allows us to analyze which workers encountered issues during the test and get insights
 * into their resource availability and capacity.
 */
public final class CheckCluster {
  // Constants should be all uppercase with underscores
  private static final AlluxioURI DIRECTORY = new AlluxioURI(AlluxioURI.SEPARATOR);
  private static final String TEST_CONTENT = "Hello Alluxio!";
  private static int sNumFiles;
  public static final String ANSI_RESET = "\u001B[0m";
  public static final String ANSI_RED = "\u001B[31m";
  public static final String ANSI_GREEN = "\u001B[32m";
  private static final List<BlockWorkerInfo> FAILED_WORKER = new ArrayList<>();
  private static WorkerClusterView sWorkerClusterView;
  private static WorkerLocationPolicy sPolicy;

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
      sWorkerClusterView = fsContext.getCachedWorkers();
      int numWorkers = sWorkerClusterView.size();
      if (numWorkers == 0) {
        System.out.println("No workers found.");
        return;
      } else if (numWorkers <= 100) {
        sNumFiles = 2000;
      } else {
        sNumFiles = numWorkers * 20;
      }

      // Test with caching
      try {
        runTestWithCaching(fs);
      } catch (Exception e) {
        System.out.println("Exception occurred during cache through test.");
        handleException(e);
      }

      System.out.println("-----------------------");

      // Test without caching (only UFS)
      try {
        runTestWithoutCaching(fs, fsContext);
      } catch (Exception e) {
        System.out.println("Exception occurred during through test.");
        handleException(e);
      }
    } catch (Exception e) {
      System.out.println(
          "Exception occurred during FileSystemContext creation or worker retrieval.");
      handleException(e);
    }
  }

  private static void handleException(Exception e) {
    System.out.println("Exception details: " + e.getMessage());
  }

  private static void setupConfiguration() {
    Configuration.set(PropertyKey.USER_WORKER_SELECTION_POLICY,
        "CONSISTENT");
    Configuration.set(PropertyKey.DORA_CLIENT_UFS_FALLBACK_ENABLED, false);
  }

  private static AlluxioURI initializeTestDirectory(FileSystem fs) throws Exception {
    FileSystem underlyingFileSystem = ((DoraCacheFileSystem) fs).getUnderlyingFileSystem();
    String testDirName = Random.class.getSimpleName() + System.currentTimeMillis();
    AlluxioURI testDir = new AlluxioURI(DIRECTORY + testDirName);
    DoraCacheFileSystem doraFs = (DoraCacheFileSystem) fs;
    testDir = doraFs.convertToUfsPath(testDir);
    if (!underlyingFileSystem.exists(testDir)) {
      underlyingFileSystem.createDirectory(testDir);
    }
    return testDir;
  }

  private static HashMap<BlockWorkerInfo, AlluxioURI> assignToWorkers(FileSystem fs,
                                                                      AlluxioURI testDir,
                                                                      boolean throughOnly)
      throws Exception {
    HashMap<BlockWorkerInfo, AlluxioURI> workerMap = new HashMap<>();
    sPolicy = WorkerLocationPolicy.Factory.create(Configuration.global());

    for (int i = 0; i < sNumFiles && workerMap.size() < sWorkerClusterView.size(); i++) {
      AlluxioURI testFile = new AlluxioURI(testDir.getPath() + "/" + i);
      writeFile(fs, testFile, throughOnly);
      List<BlockWorkerInfo> assignedWorkers =
          sPolicy.getPreferredWorkers(sWorkerClusterView, testFile.getPath(), 1);
      if (workerMap.containsKey(assignedWorkers.get(0))) {
        fs.delete(testFile, DeletePOptions.newBuilder().setRecursive(true).build());
      } else if (!FAILED_WORKER.contains(assignedWorkers.get(0))) {
        workerMap.put(assignedWorkers.get(0), testFile);
      } else {
        Iterator<WorkerInfo> iterator = sWorkerClusterView.iterator();
        while (iterator.hasNext()) {
          WorkerInfo worker = iterator.next();
          for (BlockWorkerInfo failedWorker : FAILED_WORKER) {
            if (failedWorker.getNetAddress().getRpcPort() == worker.getAddress().getRpcPort()) {
              iterator.remove();
              break;
            }
          }
        }
      }
    }
    return workerMap;
  }

  private static void writeFile(FileSystem fs, AlluxioURI testFile, boolean throughOnly)
      throws ResourceExhaustedException {
    WriteType writeType = throughOnly ? WriteType.THROUGH : WriteType.CACHE_THROUGH;
    try (FileOutStream outStream = fs.createFile(testFile,
        CreateFilePOptions.newBuilder().setWriteType(writeType.toProto()).build())) {
      outStream.write(TEST_CONTENT.getBytes());
    } catch (Exception e) {
      List<BlockWorkerInfo> workers =
          sPolicy.getPreferredWorkers(sWorkerClusterView, testFile.getPath(), 1);
      if (!FAILED_WORKER.contains(workers.get(0))) {
        FAILED_WORKER.add(workers.get(0));
      }
    }
  }

  private static void verifyFileAssignments(FileSystem fs,
                                            HashMap<BlockWorkerInfo, AlluxioURI> workerMap,
                                            boolean throughOnly) throws Exception {
    for (Map.Entry<BlockWorkerInfo, AlluxioURI> entry : workerMap.entrySet()) {
      BlockWorkerInfo worker = entry.getKey();
      AlluxioURI testFile = entry.getValue();
      URIStatus fileInfo = fs.getStatus(testFile);
      boolean isFailure = false;

      if (throughOnly && fileInfo.getInAlluxioPercentage() != 0) {
        isFailure = true;
      } else if (!throughOnly && fileInfo.getInAlluxioPercentage() != 100) {
        isFailure = true;
      }

      if (isFailure) {
        FAILED_WORKER.add(worker);
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
      }
    }
  }

  private static void printInfo() {
    int successfulWorkers = sWorkerClusterView.size();
    int failedWorkers = FAILED_WORKER.size();
    int totalWorkers = successfulWorkers + failedWorkers;
    System.out.println("\nTotal workers: " + totalWorkers);
    System.out.println(ANSI_GREEN + "Successful workers: " + successfulWorkers + ANSI_RESET);
    System.out.println(ANSI_RED + "Failed workers: " + failedWorkers + ANSI_RESET);
    if (failedWorkers == 0) {
      return;
    }
    System.out.println("\nList of failed workers:");

    for (BlockWorkerInfo failedWorker : FAILED_WORKER) {
      String hostname = failedWorker.getNetAddress().getHost();
      int port = failedWorker.getNetAddress().getRpcPort();
      long availableSpace = failedWorker.getCapacityBytes() - failedWorker.getUsedBytes();
      long capacity = failedWorker.getCapacityBytes();

      String workerInfo =
          String.format("[%s:%d] Available space %d/%d", hostname, port, availableSpace, capacity);
      System.out.println(workerInfo);
    }
  }

  private static void runTestWithCaching(FileSystem fs) throws Exception {
    AlluxioURI testDir = initializeTestDirectory(fs);
    try {
      HashMap<BlockWorkerInfo, AlluxioURI> workerMap = assignToWorkers(fs, testDir, false);
      verifyFileAssignments(fs, workerMap, false);
      for (Map.Entry<BlockWorkerInfo, AlluxioURI> entry : workerMap.entrySet()) {
        verifyFileContent(fs, entry.getValue());
      }
      System.out.println("Test with CACHE_THROUGH write type");
      printInfo();
    } catch (Exception e) {
      System.out.println("Exception occurred during cache through test.");
      handleException(e);
    } finally {
      // Clean up the test directory and its contents
      try {
        fs.delete(testDir, DeletePOptions.newBuilder().setRecursive(true).build());
      } catch (Exception e) {
        System.out.println("Failed to clean up the test directory.");
        handleException(e);
      }
    }
  }

  private static void runTestWithoutCaching(FileSystem fs, FileSystemContext fsContext)
      throws Exception {
    System.out.println("Test with THROUGH write type");
    FAILED_WORKER.clear();
    sWorkerClusterView = fsContext.getCachedWorkers();
    AlluxioURI testDir = initializeTestDirectory(fs);
    try {
      HashMap<BlockWorkerInfo, AlluxioURI> workerMap = assignToWorkers(fs, testDir, true);
      verifyFileAssignments(fs, workerMap, true);
      for (Map.Entry<BlockWorkerInfo, AlluxioURI> entry : workerMap.entrySet()) {
        verifyFileContent(fs, entry.getValue());
      }
      printInfo();
    } catch (Exception e) {
      System.out.println("Exception occurred during through test.");
      handleException(e);
    } finally {
      // Clean up the test directory and its contents
      try {
        fs.delete(testDir, DeletePOptions.newBuilder().setRecursive(true).build());
      } catch (Exception e) {
        System.out.println("Failed to clean up the test directory.");
        handleException(e);
      }
    }
  }
}
