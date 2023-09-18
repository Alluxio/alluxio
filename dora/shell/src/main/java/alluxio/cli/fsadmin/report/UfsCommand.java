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
import alluxio.wire.MountPointInfo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Prints under filesystem information.
 */
public class UfsCommand {
  private FileSystemMasterClient mFileSystemMasterClient;
  private static final Logger LOG = LoggerFactory.getLogger(JobServiceMetricsCommand.class);

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
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      String json = objectMapper.writeValueAsString(mountTable);
      System.out.println(json);
    } catch (JsonProcessingException e) {
      System.out.println("Failed to convert mountTable output to JSON. "
          + "Check the command line log for the detailed error message.");
      LOG.error("Failed to output JSON object {}", mountTable);
      e.printStackTrace();
      return -1;
    }
    return 0;
  }
}
