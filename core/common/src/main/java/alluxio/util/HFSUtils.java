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

import alluxio.Constants;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * This util class is called in alluxio-mount.sh to convert the memory size to number of sectors.
 */
public final class HFSUtils {

  /**
   * Convert the memory size to number of sectors.
   *
   * @param requestSize the size of memory in bytes
   * @param sectorSize the size of each sector in bytes
   * @return 0 if getNumSectors execute successfully
   */
  public static BigDecimal getNumSector(String requestSize, String sectorSize) {
    // Metadata zone size is estimated by using specifications noted at
    // http://dubeiko.com/development/FileSystems/HFSPLUS/tn1150.html#MetadataZone,
    // which is slightly larger than actually metadata zone size created by hdiuitl command.
    BigDecimal memSize = new BigDecimal(requestSize);
    BigDecimal sectorBytes = new BigDecimal(sectorSize);

    BigDecimal sizeKB = new BigDecimal(Constants.KB);
    BigDecimal sizeMB = new BigDecimal(Constants.MB);
    BigDecimal sizeGB = new BigDecimal(Constants.GB);
    BigDecimal hundredGB = sizeGB.multiply(BigDecimal.valueOf(100));

    BigDecimal nSectors = memSize.divide(sectorBytes);
    BigDecimal memSizeKB = memSize.divide(sizeKB, 4, RoundingMode.HALF_UP); // round up
    BigDecimal memSizeGB = memSize.divide(sizeGB, 4, RoundingMode.HALF_UP); // round up
    BigDecimal memSize100GB = memSize.divide(hundredGB, 8, RoundingMode.HALF_UP); // round up

    // allocation bitmap file: one bit per sector
    BigDecimal allocBitmapSize = nSectors.divide(BigDecimal.valueOf(8));

    // extend overflow file: 4MB, plus 4MB per 100GB
    BigDecimal extOverflowFileSize = memSize100GB.multiply(sizeMB).multiply(BigDecimal.valueOf(4));

    // journal file: 8MB, plus 8MB per 100GB
    BigDecimal journalFileSize = memSize100GB.multiply(sizeMB).multiply(BigDecimal.valueOf(8));

    // catalog file: 10bytes per KB
    BigDecimal catalogFileSize = memSizeKB.multiply(BigDecimal.valueOf(10));

    // hot files: 5bytes per KB
    BigDecimal hotFileSize = memSizeKB.multiply(BigDecimal.valueOf(5));

    // quota users file and quota groups file
    // quotaUsersFileSize = (memSizeGB * 256 + 1) * 64;
    // quotaGroupsFileSize = (memSizeGB * 32 + 1) * 64;
    BigDecimal quotaUsersFileSize = memSizeGB.multiply(BigDecimal.valueOf(16384))
        .add(BigDecimal.valueOf(64));
    BigDecimal quotaGroupsFileSize = memSizeGB.multiply(BigDecimal.valueOf(2048))
        .add(BigDecimal.valueOf(64));

    BigDecimal metadataSize = allocBitmapSize.add(extOverflowFileSize)
        .add(journalFileSize).add(catalogFileSize).add(hotFileSize)
        .add(quotaUsersFileSize).add(quotaGroupsFileSize);

    BigDecimal allocSize = memSize.add(metadataSize);
    BigDecimal numSectors = allocSize.divide(sectorBytes, 0, RoundingMode.UP);

    System.out.println(numSectors);
    return numSectors;
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
