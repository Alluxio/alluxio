/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.yarn;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 * YARN related utils.
 */
public final class Utils {
  private Utils() {} // prevent instantiation

  /**
   * Creates a local resource for a file on HDFS.
   *
   * @param yarnConf YARN configuration
   * @param resourcePath known path of resource file on HDFS
   * @throws IOException if the file can not be found on HDFS
   */
  public static LocalResource createLocalResourceOfFile(YarnConfiguration yarnConf,
      String resourcePath) throws IOException {
    LocalResource localResource = Records.newRecord(LocalResource.class);

    Path jarHdfsPath = new Path(resourcePath);
    FileStatus jarStat = FileSystem.get(yarnConf).getFileStatus(jarHdfsPath);
    localResource.setResource(ConverterUtils.getYarnUrlFromPath(jarHdfsPath));
    localResource.setSize(jarStat.getLen());
    localResource.setTimestamp(jarStat.getModificationTime());
    localResource.setType(LocalResourceType.FILE);
    localResource.setVisibility(LocalResourceVisibility.PUBLIC);
    return localResource;
  }
}
