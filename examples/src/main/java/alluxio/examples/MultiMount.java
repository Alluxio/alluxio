/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.examples;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;

import org.apache.commons.io.IOUtils;

/**
 * Example program that demonstrates Alluxio's ability to read and write data across different
 * types of storage systems. In particular, this example reads data from S3 and writes data to HDFS.
 *
 * NOTE: To run this example, you must replace the "hdfs://localhost:9000/" URL with a
 * URL of a valid HDFS cluster. Also, you need to set the fs.s3n.awsAccessKeyId and
 * fs.s3.awsSecretAccessKey VM properties to a valid AWS access key ID and aws secret access key
 * respectively in order to access the S3 bucket the data is read from.
 */
public final class MultiMount {

  /**
   * Entry point for the {@link MultiMount} program.
   *
   * @param args command-line arguments
   */
  public static void main(String []args) {
    FileSystem fileSystem = FileSystem.Factory.get();
    try {
      AlluxioURI mntPath = new AlluxioURI("/mnt");
      AlluxioURI s3Path = new AlluxioURI("/mnt/s3");
      AlluxioURI hdfsPath = new AlluxioURI("/mnt/hdfs");
      // Make sure that the directory under which mount points are created exists.
      if (!fileSystem.exists(mntPath)) {
        fileSystem.createDirectory(mntPath);
      }
      // Make sure that the S3 mount point does not exist.
      if (fileSystem.exists(s3Path)) {
        fileSystem.unmount(s3Path);
      }
      // Make sure that the HDFS mount point does not exist.
      if (fileSystem.exists(hdfsPath)) {
        fileSystem.unmount(hdfsPath);
      }
      // Create the S3 and HDFS mount points.
      fileSystem.mount(new AlluxioURI("/mnt/s3"), new AlluxioURI("s3n://alluxio-demo/"));
      fileSystem.mount(new AlluxioURI("/mnt/hdfs"), new AlluxioURI("hdfs://localhost:9000/"));
      AlluxioURI inputPath = new AlluxioURI("/mnt/s3/hello.txt");
      AlluxioURI outputPath = new AlluxioURI("/mnt/hdfs/hello.txt");
      // Make sure the output does not exist.
      if (fileSystem.exists(outputPath)) {
        fileSystem.delete(outputPath);
      }
      FileInStream is = fileSystem.openFile(inputPath);
      // Make sure that results are persisted to HDFS.
      CreateFileOptions options =
          CreateFileOptions.defaults().setWriteType(WriteType.CACHE_THROUGH);
      FileOutStream os = fileSystem.createFile(outputPath, options);
      // Copy the data.
      IOUtils.copy(is, os);
      is.close();
      os.close();
      // Finally, remove the mount points.
      fileSystem.unmount(new AlluxioURI("/mnt/s3"));
      fileSystem.unmount(new AlluxioURI("/mnt/hdfs"));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
