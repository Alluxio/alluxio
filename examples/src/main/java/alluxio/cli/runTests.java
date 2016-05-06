/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.cli;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.examples.BasicNonByteBufferOperations;
import alluxio.examples.BasicOperations;
import alluxio.examples.Utils;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Driver to run Alluxio tests.
 */
@NotThreadSafe
public final class RunTests {
  /**
   * The operation types to test.
   */
  enum OperationType {
    /**
     * Basic operations.
     */
    BasicOperations,
    /**
     * Basic operations but not using ByteBuffer.
     */
    BasicNonByteBufferOperations,
  }

  private static void usage() {
    System.out.println("Usage:");
    System.out.println(
        "java -cp " + Version.ALLUXIO_JAR + " " + RunTests.class.getName() + " <master address>");
  }

  /**
   * Console program that validates the configuration.
   *
   * @param args there are no arguments needed
   */
  public static void main(String[] args) {
    if (args.length != 1) {
      usage();
      System.exit(-1);
    }
    AlluxioURI masterLocation = new AlluxioURI(args[0]);

    int failed = 0;
    for (ReadType readType : ReadType.values()) {
      for (WriteType writeType : WriteType.values()) {
        for (OperationType opType : OperationType.values()) {
          System.out.println(
              String.format("readType=%s, writeType=%s, opType=%s", readType, writeType, opType));
          if (!runTest(masterLocation, readType, writeType, opType)) {
            failed++;
          }
        }
      }
    }
    if (failed > 0) {
      System.out.println("Number of failed tests: " + failed);
    }
    System.exit(failed);
  }

  private static boolean runTest(AlluxioURI masterLocation, ReadType readType, WriteType writeType,
      OperationType opType) {
    AlluxioURI filePath =
        new AlluxioURI(String.format("/default_tests_files/%s_%s_%s", opType, readType, writeType));
    boolean result;
    if (opType == OperationType.BasicOperations) {
      result = Utils.runExample(new BasicOperations(masterLocation, filePath, readType, writeType));
    } else {
      result = Utils.runExample(
          new BasicNonByteBufferOperations(masterLocation, filePath, readType, writeType, true,
              20));
    }
    return result;
  }
}
