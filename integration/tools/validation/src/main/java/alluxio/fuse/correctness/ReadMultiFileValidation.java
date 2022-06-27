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

import static alluxio.fuse.correctness.Utils.RANDOM;

import java.util.ArrayList;
import java.util.List;

/**
 * This class validates multiple-file read correctness of AlluxioFuse.
 */
public class ReadMultiFileValidation {
  /**
   * The whole process of validating reading correctness on multiple files.
   *
   * @param options the options for the validation test
   */
  public static void run(Options options) {
    for (long fileSize : Constants.FILE_SIZES) {
      System.out.println(String.format(
          Constants.TESTING_FILE_SIZE_FORMAT, Constants.READ, fileSize));
      List<String> localFileList = new ArrayList<>(options.getNumFiles());
      List<String> fuseFileList = new ArrayList<>(options.getNumFiles());
      createTestFiles(fileSize, options, localFileList, fuseFileList);
      for (int bufferSize : Constants.BUFFER_SIZES) {
        validate(Constants.SEQUENTIAL_READ,
            bufferSize, options.getNumThreads(), localFileList, fuseFileList);
        validate(Constants.RANDOM_READ,
            bufferSize, options.getNumThreads(), localFileList, fuseFileList);
        validate(Constants.MIXED_READ,
            bufferSize, options.getNumThreads(), localFileList, fuseFileList);
      }
      deleteTestFiles(localFileList, fuseFileList);
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

  private static void validate(String mode, int bufferSize, int numThreads,
      List<String> localFileList, List<String> fuseFileList) {
    List<Thread> threads = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      Thread t = new Thread(() -> {
        while (true) {
          int index = RANDOM.nextInt(localFileList.size());
          try {
            ReadSingleFileValidation.validate(
                mode, localFileList.get(index), fuseFileList.get(index), 1, bufferSize);
          } catch (InterruptedException e) {
            return;
          }
        }
      });
      threads.add(t);
    }
    for (Thread t : threads) {
      t.start();
    }
    try {
      Thread.sleep(1 * 60 * 1000);
    } catch (InterruptedException e) {
      System.out.println(Constants.THREAD_INTERRUPTED_MESSAGE);
    } finally {
      for (Thread t : threads) {
        t.interrupt();
      }
    }
  }

  private static void deleteTestFiles(List<String> localFileList, List<String> fuseFileList) {
    for (int i = 0; i < localFileList.size(); i++) {
      Utils.deleteTestFiles(localFileList.get(i), fuseFileList.get(i));
    }
  }
}
