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

import static alluxio.Constants.GB;
import static alluxio.Constants.KB;
import static alluxio.Constants.MB;

/**
 * This class holds all the constants for Fuse correctness validation tests.
 */
public class Constants {
  public static final long[] FILE_SIZES = {100 * KB, MB, 1059062, 63 * MB, 65 * MB, GB, 10L * GB};
  public static final int[] BUFFER_SIZES = {
      128, 1000, 1001, MB, 1025, 4 * KB, 32 * KB, 128 * KB, MB, 4 * MB};
  public static final int DEFAULT_BUFFER_SIZE = MB;
  public static final String TESTING_FILE_SIZE_FORMAT = "Starting testing %s of file size %d.";
  public static final String DATA_INCONSISTENCY_FORMAT =
      "Data inconsistency found while testing %s with buffer size %d.";
  public static final String THREAD_INTERRUPTED_MESSAGE =
      "Some thread is interrupted. Test is stopped.";
  public static final String READ = "read";
  public static final String RANDOM_READ = "random read";
  public static final String SEQUENTIAL_READ = "sequential read";
  public static final String MIXED_READ = "mixed read";
  public static final String SEQUENTIAL_WRITE = "sequential write";

  private Constants() {}
}
