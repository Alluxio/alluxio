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

package alluxio.util;

/**
 * Utility method called in alluxio-mount.sh to calculate space for Mac OS X HFS+.
 * Metadata zone size is estimated by using specifications noted at
 * http://dubeiko.com/development/FileSystems/HFSPLUS/tn1150.html#MetadataZone,
 * which is slightly larger than actually metadata zone size created by hdiuitl command.
 */
public final class HFSUtils {

  /**
   * Converts the memory size to number of sectors.
   *
   * @param requestSize requested filesystem size in bytes
   * @param sectorSize the size of each sector in bytes
   * @return total sectors of HFS+ including estimated metadata zone size
   */
  public static long getNumSector(String requestSize, String sectorSize) {
    Double memSize = Double.parseDouble(requestSize);
    Double sectorBytes = Double.parseDouble(sectorSize);
    Double nSectors = memSize / sectorBytes;
    Double memSizeKB = memSize / 1024;
    Double memSizeGB = memSize / (1024 * 1024 * 1024);
    Double memSize100GB = memSizeGB / 100;

    // allocation bitmap file: one bit per sector
    Double allocBitmapSize = nSectors / 8;

    // extend overflow file: 4MB, plus 4MB per 100GB
    Double extOverflowFileSize = memSize100GB * 1024 * 1024 * 4;

    // journal file: 8MB, plus 8MB per 100GB
    Double journalFileSize = memSize100GB * 1024 * 1024 * 8;

    // catalog file: 10bytes per KB
    Double catalogFileSize = memSizeKB * 10;

    // hot files: 5bytes per KB
    Double hotFileSize = memSizeKB * 5;

    // quota users file and quota groups file
    Double quotaUsersFileSize = (memSizeGB * 256 + 1) * 64;
    Double quotaGroupsFileSize = (memSizeGB * 32 + 1) * 64;
    Double metadataSize = allocBitmapSize + extOverflowFileSize + journalFileSize
        + catalogFileSize + hotFileSize + quotaUsersFileSize + quotaGroupsFileSize;

    Double allocSize = memSize + metadataSize;
    Double numSectors = allocSize / sectorBytes;

    System.out.println(numSectors.longValue() + 1); // round up
    return numSectors.longValue() + 1;
  }

  /**
   * The main class to invoke the getNumSector.
   *
   * @param args the arguments parsed by commandline
   */
  public static void main(String[] args) {
    if (args.length != 2) {
      System.exit(-1);
    }
    String mem = args[0];
    String sector = args[1];
    try {
      getNumSector(mem, sector);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private HFSUtils() {} // prevent instantiation
}
