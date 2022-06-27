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
 * This class validates single-file read correctness of AlluxioFuse.
 */
public class ReadSingleFileValidation {
  /**
   * The whole process of validating reading correctness on a single file.
   *
   * @param options the options for the validation test
   */
  public static void run(Options options) {
    for (long fileSize: Constants.FILE_SIZES) {
      System.out.println(String.format(
          Constants.TESTING_FILE_SIZE_FORMAT, Constants.READ, fileSize));
      String localFilePath = Utils.createFile(fileSize, options.getLocalDir());
      String fuseFilePath =
          Utils.copyLocalFileToFuseMountPoint(localFilePath, options.getFuseDir());
      try {
        for (int bufferSize: Constants.BUFFER_SIZES) {
          validate(Constants.SEQUENTIAL_READ,
              localFilePath, fuseFilePath, options.getNumThreads(), bufferSize);
          validate(Constants.RANDOM_READ,
              localFilePath, fuseFilePath, options.getNumThreads(), bufferSize);
          if (options.getNumThreads() > 1) {
            validate(Constants.MIXED_READ,
                localFilePath, fuseFilePath, options.getNumThreads(), bufferSize);
          }
        }
      } catch (InterruptedException e) {
        throw new RuntimeException("Some thread is interrupted. Test is stopped");
      } finally {
        Utils.deleteTestFiles(localFilePath, fuseFilePath);
      }
    }
  }

  /**
   * Validating reading correctness on a single files given configurations.
   *
   * @param mode          the read pattern
   * @param localFilePath the path of the local testing file
   * @param fuseFilePath  the path of the fuse testing file
   * @param numThreads    number of threads reading
   * @param bufferSize    size of the buffer used by the threads
   */
  public static void validate(String mode,
      String localFilePath, String fuseFilePath, int numThreads, int bufferSize)
      throws InterruptedException {
    List<Thread> threads = new ArrayList<>(numThreads);
    if (mode.equals(Constants.MIXED_READ)) {
      for (int i = 0; i < numThreads / 2; i++) {
        threads.add(new Thread(
            new SequentialReadClient(localFilePath, fuseFilePath, bufferSize)));
      }
      for (int i = numThreads / 2; i < numThreads; i++) {
        threads.add(new Thread(
            new RandomReadClient(localFilePath, fuseFilePath, bufferSize)));
      }
    } else {
      for (int i = 0; i < numThreads; i++) {
        switch (mode) {
          case Constants.SEQUENTIAL_READ:
            threads.add(
                new Thread(new SequentialReadClient(localFilePath, fuseFilePath, bufferSize)));
            break;
          case Constants.RANDOM_READ:
            threads.add(
                new Thread(new RandomReadClient(localFilePath, fuseFilePath, bufferSize)));
            break;
          default:
            throw new RuntimeException(String.format("Invalid data access pattern %s", mode));
        }
      }
    }
    for (Thread t: threads) {
      t.start();
    }
    try {
      for (Thread t: threads) {
        t.join();
      }
    } catch (InterruptedException e) {
      for (Thread t: threads) {
        t.interrupt();
      }
      throw e;
    }
  }
}
