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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Comparator;
import java.util.Map;

/**
 * Prints under filesystem information.
 */
public class UfsCommand {
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
    ObjectMapper mapper = new ObjectMapper();
    ArrayNode ufsInfo = mapper.createArrayNode();

    Map<String, MountPointInfo> mountTable = mFileSystemMasterClient.getMountTable();
    for (Map.Entry<String, MountPointInfo> entry : mountTable.entrySet()) {
      ObjectNode mountInfo = mapper.createObjectNode();

      String mountPoint = entry.getKey();
      MountPointInfo mountPointInfo = entry.getValue();
      long capacityBytes = mountPointInfo.getUfsCapacityBytes();
      long usedBytes = mountPointInfo.getUfsUsedBytes();
      String usedPercentageInfo = "";
      if (capacityBytes > 0) {
        int usedPercentage = (int) (100.0 * usedBytes / capacityBytes);
        usedPercentageInfo = String.format("(%s%%)", usedPercentage);
      }

      mountInfo.put("URI", mountPointInfo.getUfsUri());
      mountInfo.put("Mount Point", mountPoint);
      mountInfo.put("UFS Type", mountPointInfo.getUfsType());
      mountInfo.put("Total Capacity", FormatUtils.getSizeFromBytes(capacityBytes));
      mountInfo.put("Used Capacity", FormatUtils.getSizeFromBytes(usedBytes) + usedPercentageInfo);
      mountInfo.put("Readonly", mountPointInfo.getReadOnly());
      mountInfo.put("Shared", mountPointInfo.getShared());

      ArrayNode props = mapper.createArrayNode();
      Map<String, String> properties = mountPointInfo.getProperties();
      for (Map.Entry<String, String> property : properties.entrySet()) {
        ObjectNode prop = mapper.createObjectNode();
        prop.put("key", property.getKey());
        prop.put("value", property.getValue());
        props.add(prop);
      }
      mountInfo.set("Properties", props);

      ufsInfo.add(mountInfo);
    }

    System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(ufsInfo));
    return 0;
  }
}
