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

package alluxio.fuse;

/**
 * Alluxio Fuse utilities to handle different fuse.rename() flags.
 */
public class AlluxioJniRenameUtils {
  public static final int NO_FLAGS = 0;
  public static final int RENAME_NOREPLACE = 1;
  public static final int RENAME_EXCHANGE = 2;

  /**
   * Checks if the rename flag contains RENAME_EXCHANGE flag.
   *
   * @param flag the rename flag to check
   * @return true if contains RENAME_EXCHANGE flag, false otherwise
   */
  public static boolean exchange(int flag) {
    return (flag & RENAME_EXCHANGE) != 0;
  }

  /**
   * Checks if the rename flag contains RENAME_NOREPLACE flag.
   *
   * @param flag the rename flag to check
   * @return true if contains RENAME_NOREPLACE flag, false otherwise
   */
  public static boolean noreplace(int flag) {
    return (flag & RENAME_NOREPLACE) != 0;
  }
}
