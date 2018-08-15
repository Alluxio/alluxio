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

package alluxio.master.backwards.compatibility;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.master.MasterClientConfig;
import alluxio.master.backwards.compatibility.TestOp.Clients;
import alluxio.multi.process.MultiProcessCluster;

public class Utils {
  public static void createFile(FileSystem fs, AlluxioURI path) throws Exception {
    try (FileOutStream out =
             fs.createFile(path, CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB))) {
      out.write("test".getBytes());
    }
  }

  public static Clients getClients(MultiProcessCluster mCluster) {
    return new Clients(mCluster.getFileSystemClient(), FileSystemMasterClient.Factory.create(
        MasterClientConfig.defaults().withMasterInquireClient(mCluster.getMasterInquireClient())));
  }

  private Utils() {} // Prevent instantiation
}
