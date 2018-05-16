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

package alluxio.cli.fsadmin.report;

import alluxio.client.file.FileSystemMasterClient;
import alluxio.util.FormatUtils;
import alluxio.wire.MountPointInfo;

import java.io.IOException;
import java.util.Map;

/**
 * Prints under filesystem information.
 */
public class UfsCommand {
  private static final String LEFT_ALIGN_FORMAT = "%-60s  on  %-20s (%s, capacity=%s,"
      + " used=%s, %sread-only, %sshared, ";

  private FileSystemMasterClient mFileSystemMasterClient;

  /**
   * Creates a new instance of {@link UfsCommand}.
   *
   * @param fileSystemMasterClient client to get mount table from
   */
  public UfsCommand(FileSystemMasterClient fileSystemMasterClient) {
    mFileSystemMasterClient = fileSystemMasterClient;
  }

  /**
   * Runs report ufs command.
   *
   * @return 0 on success, 1 otherwise
   */
  public int run() throws IOException {
    Map<String, MountPointInfo> mountTable = mFileSystemMasterClient.getMountTable();
    System.out.println("Alluxio under filesystem information: ");
    printMountInfo(mountTable);
    // TODO(lu) add UfsSessionCount metrics information
    return 0;
  }

  /**
   * Prints mount information for a mount table.
   *
   * @param mountTable the mount table to get information from
   */
  public static void printMountInfo(Map<String, MountPointInfo> mountTable) {
    for (Map.Entry<String, MountPointInfo> entry : mountTable.entrySet()) {
      String mMountPoint = entry.getKey();
      MountPointInfo mountPointInfo = entry.getValue();

      long capacityBytes = mountPointInfo.getUfsCapacityBytes();
      long usedBytes = mountPointInfo.getUfsUsedBytes();

      String usedPercentageInfo = "";
      if (capacityBytes > 0) {
        int usedPercentage = (int) (100L * usedBytes / capacityBytes);
        usedPercentageInfo = String.format("(%s%%)", usedPercentage);
      }

      System.out.format(LEFT_ALIGN_FORMAT, mountPointInfo.getUfsUri(), mMountPoint,
          mountPointInfo.getUfsType(), FormatUtils.getSizeFromBytes(capacityBytes),
          FormatUtils.getSizeFromBytes(usedBytes) + usedPercentageInfo,
          mountPointInfo.getReadOnly() ? "" : "not ",
          mountPointInfo.getShared() ? "" : "not ");
      System.out.println("properties=" + mountPointInfo.getProperties() + ")");
    }
  }
}
