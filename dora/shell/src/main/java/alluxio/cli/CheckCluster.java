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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private static final List<BlockWorkerInfo> FAILED_WORKER = new ArrayList<>();
  private static List<BlockWorkerInfo> sWorkerInfoList;
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
      sWorkerInfoList = fsContext.getCachedWorkers();

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
        "alluxio.client.file.dora.ConsistentHashPolicy");
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

    for (int i = 0; i < NUM_FILES && workerMap.size() < sWorkerInfoList.size(); i++) {
      AlluxioURI testFile = new AlluxioURI(testDir.getPath() + "/" + i);
      writeFile(fs, testFile, throughOnly);
      List<BlockWorkerInfo> assignedWorkers =
          sPolicy.getPreferredWorkers(sWorkerInfoList, testFile.getPath(), 1);
      if (workerMap.containsKey(assignedWorkers.get(0))) {
        fs.delete(testFile, DeletePOptions.newBuilder().setRecursive(true).build());
      } else if (!FAILED_WORKER.contains(assignedWorkers.get(0))) {
        workerMap.put(assignedWorkers.get(0), testFile);
      } else {
        for (BlockWorkerInfo worker : sWorkerInfoList) {
          for (BlockWorkerInfo sWorker : FAILED_WORKER) {
            if (sWorker.getNetAddress().getRpcPort() == worker.getNetAddress().getRpcPort()) {
              sWorkerInfoList.remove(worker);
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
          sPolicy.getPreferredWorkers(sWorkerInfoList, testFile.getPath(), 1);
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
    int successfulWorkers = sWorkerInfoList.size();
    int failedWorkers = FAILED_WORKER.size();
    int totalWorkers = successfulWorkers + failedWorkers;
    System.out.println("\nTotal workers: " + totalWorkers);
    System.out.println(ANSI_GREEN + "Successful workers: " + successfulWorkers + ANSI_RESET);
    System.out.println(ANSI_RED + "Failed workers: " + failedWorkers + ANSI_RESET);

    if (!FAILED_WORKER.isEmpty()) {
      System.out.println(ANSI_RED + "\nList of failed workers:" + ANSI_RESET);
      for (BlockWorkerInfo failedWorker : FAILED_WORKER) {
        System.out.println("Failed worker capacity and port:");
        System.out.println(ANSI_RED + "Capacity:" + failedWorker.getCapacityBytes() + " Port:"
            + failedWorker.getNetAddress().getRpcPort() + ANSI_RESET);
      }
    }
  }

  private static void runTestWithCaching(FileSystem fs) throws Exception {
    AlluxioURI testDir = initializeTestDirectory(fs);
    HashMap<BlockWorkerInfo, AlluxioURI> workerMap = assignToWorkers(fs, testDir, false);
    verifyFileAssignments(fs, workerMap, false);
    for (Map.Entry<BlockWorkerInfo, AlluxioURI> entry : workerMap.entrySet()) {
      verifyFileContent(fs, entry.getValue());
    }
    System.out.println("cache through test");
    printInfo();
  }

  private static void runTestWithoutCaching(FileSystem fs, FileSystemContext fsContext)
      throws Exception {
    System.out.println("through test");
    FAILED_WORKER.clear();
    sWorkerInfoList = fsContext.getCachedWorkers();
    AlluxioURI testDir = initializeTestDirectory(fs);
    HashMap<BlockWorkerInfo, AlluxioURI> workerMap = assignToWorkers(fs, testDir, true);
    verifyFileAssignments(fs, workerMap, true);
    for (Map.Entry<BlockWorkerInfo, AlluxioURI> entry : workerMap.entrySet()) {
      verifyFileContent(fs, entry.getValue());
    }
    printInfo();
  }
}
