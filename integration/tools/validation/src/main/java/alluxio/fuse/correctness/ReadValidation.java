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

package alluxio.fuse.correctness;

import alluxio.fuse.correctness.client.RandomReadClient;
import alluxio.fuse.correctness.client.SequentialReadClient;

import java.util.ArrayList;
import java.util.List;

/**
 * This class validates the read correctness of AlluxioFuse.
 */
public class ReadValidation {
  /**
   * Starts validating read correctness of AlluxioFuse.
   *
   * @param options contains the options for the test
   */
  public static void run(Options options) {
    for (long fileSize: Constants.FILE_SIZES) {
      System.out.println(String.format(
          Constants.TESTING_FILE_SIZE_FORMAT, IOOperation.Read, fileSize));
      List<String> localFileList = new ArrayList<>(options.getNumFiles());
      List<String> fuseFileList = new ArrayList<>(options.getNumFiles());
      createTestFiles(fileSize, options, localFileList, fuseFileList);
      try {
        for (int bufferSize: Constants.BUFFER_SIZES) {
          validate(IOOperation.SequentialRead,
              localFileList, fuseFileList, bufferSize, options);
          validate(IOOperation.RandomRead,
              localFileList, fuseFileList, bufferSize, options);
          if (options.getNumThreads() > 1) {
            validate(IOOperation.MixedRead,
                localFileList, fuseFileList, bufferSize, options);
          }
        }
      } catch (InterruptedException e) {
        throw new RuntimeException("Some thread is interrupted. Test is stopped");
      } finally {
        deleteTestFiles(localFileList, fuseFileList);
      }
    }
  }

  private static void createTestFiles(long fileSize, Options options,
      List<String> localFileList, List<String> fuseFileList) {
    for (int i = 0; i < options.getNumFiles(); i++) {
      String localFilePath = Utils.createFile(fileSize, options.getLocalDir());
      String fuseFilePath = Utils.copyLocalFileToFuseMountPoint(
          localFilePath, options.getFuseDir());
      localFileList.add(localFilePath);
      fuseFileList.add(fuseFilePath);
    }
  }

  private static void validate(IOOperation operation, List<String> localFileList,
      List<String> fuseFileList, int bufferSize, Options options)
      throws InterruptedException {
    int numThreads = options.getNumThreads();
    boolean longRunningClient = options.getNumFiles() > 1;
    List<Thread> threads = new ArrayList<>(numThreads);
    if (operation.equals(IOOperation.MixedRead)) {
      for (int i = 0; i < numThreads / 2; i++) {
        threads.add(new Thread(
            new SequentialReadClient(localFileList, fuseFileList, bufferSize, longRunningClient)));
      }
      for (int i = numThreads / 2; i < numThreads; i++) {
        threads.add(new Thread(
            new RandomReadClient(localFileList, fuseFileList, bufferSize, longRunningClient)));
      }
    } else {
      for (int i = 0; i < numThreads; i++) {
        if (operation == IOOperation.SequentialRead) {
          threads.add(new Thread(new SequentialReadClient(
              localFileList, fuseFileList, bufferSize, longRunningClient)));
        } else if (operation == IOOperation.RandomRead) {
          threads.add(new Thread(new RandomReadClient(
              localFileList, fuseFileList, bufferSize, longRunningClient)));
        } else {
          throw new RuntimeException(String.format("Invalid data access pattern %s", operation));
        }
      }
    }
    for (Thread t: threads) {
      t.start();
    }
    if (longRunningClient) {
      interruptClientsAfterTimeout(threads, options.getTimeout());
    } else {
      waitForClientsToFinish(threads);
    }
  }

  private static void interruptClientsAfterTimeout(List<Thread> threads, long timeout) {
    try {
      Thread.sleep(timeout);
    } catch (InterruptedException e) {
      throw new RuntimeException(Constants.THREAD_INTERRUPTED_MESSAGE);
    } finally {
      for (Thread t: threads) {
        t.interrupt();
      }
    }
  }

  private static void waitForClientsToFinish(List<Thread> threads) {
    try {
      for (Thread t: threads) {
        t.join();
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(Constants.THREAD_INTERRUPTED_MESSAGE);
    }
  }

  private static void deleteTestFiles(List<String> localFileList, List<String> fuseFileList) {
    for (int i = 0; i < localFileList.size(); i++) {
      Utils.deleteTestFiles(localFileList.get(i), fuseFileList.get(i));
    }
  }
}
