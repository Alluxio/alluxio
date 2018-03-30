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

package alluxio.cli.fsadmin;

/**
 * Class for convenience methods used by {@link FileSystemAdminShell}.
 */
public final class FileSystemAdminShellUtils {

  private FileSystemAdminShellUtils() {} // prevent instantiation

  /**
   * Compares two tier names according to their rank values.
   *
   * @param a one tier name
   * @param b another tier name
   * @return compared result
   */
  public static int compareTierNames(String a, String b) {
    int aValue = getTierRankValue(a);
    int bValue = getTierRankValue(b);
    if (aValue == bValue) {
      return a.compareTo(b);
    }
    return bValue - aValue;
  }

  /**
   * Assigns a rank value to the input string.
   *
   * @param input the input to turn to rank value
   * @return a rank value used to sort tiers
   */
  private static int getTierRankValue(String input) {
    // MEM, SSD, and HDD are the most commonly used Alluxio tier alias,
    // so we want them to show before other tier names
    // MEM, SSD, and HDD are sorted according to speed of access
    switch (input) {
      case "MEM":
        return 4;
      case "SSD":
        return 3;
      case "HDD":
        return 2;
      default:
        return 0;
    }
  }
}
