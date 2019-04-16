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

package alluxio.examples;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.util.ConfigurationUtils;

import org.apache.commons.io.IOUtils;

/**
 * Example program that demonstrates Alluxio's ability to read and write data across different
 * types of storage systems. In particular, this example reads data from S3 and writes data to HDFS.
 *
 * NOTE: This example assumes that the existence of an Alluxio cluster, the existence of an HDFS
 * cluster, and that the properties for accessing S3 are set in your Alluxio configuration.
 *
 * If the above assumptions are met, you can run the example using:
 *
 * ./bin/alluxio runClass alluxio.examples.MultiMount <HDFS_URL>
 */
public final class MultiMount {

  private MultiMount() {} // prevent instantiation

  /**
   * Entry point for the {@link MultiMount} program.
   *
   * @param args command-line arguments
   */
  public static void main(String []args) {
    if (args.length != 1) {
      System.err.println("Usage: ./bin/alluxio runClass alluxio.examples.MultiMount <HDFS_URL>");
      System.exit(-1);
    }

    AlluxioConfiguration alluxioConf = new InstancedConfiguration(ConfigurationUtils.defaults());

    AlluxioURI mntPath = new AlluxioURI("/mnt");
    AlluxioURI s3Mount = new AlluxioURI("/mnt/s3");
    AlluxioURI inputPath = new AlluxioURI("/mnt/s3/hello.txt");
    AlluxioURI s3Path = new AlluxioURI("s3a://alluxio-demo/");
    AlluxioURI hdfsMount = new AlluxioURI("/mnt/hdfs");
    AlluxioURI outputPath = new AlluxioURI("/mnt/hdfs/hello.txt");
    AlluxioURI hdfsPath = new AlluxioURI(args[0]);
    FileSystem fileSystem = FileSystem.Factory.create(alluxioConf);

    try {
      // Make sure mount directory exists.
      if (!fileSystem.exists(mntPath)) {
        System.out.print("creating " + mntPath + " ... ");
        fileSystem.createDirectory(mntPath);
        System.out.println("done");
      }

      // Make sure the S3 mount point does not exist.
      if (fileSystem.exists(s3Mount)) {
        System.out.print("unmounting " + s3Mount + " ... ");
        fileSystem.unmount(s3Mount);
        System.out.println("done");
      }

      // Make sure the HDFS mount point does not exist.
      if (fileSystem.exists(hdfsMount)) {
        System.out.print("unmounting " + hdfsMount + " ... ");
        fileSystem.unmount(hdfsMount);
        System.out.println("done");
      }

      // Mount S3.
      System.out.print("mounting " + s3Path + " to " + s3Mount + " ... ");
      fileSystem.mount(s3Mount, s3Path);
      System.out.println("done");

      // Mount HDFS.
      System.out.print("mounting " + hdfsPath + " to " + hdfsMount + " ... ");
      fileSystem.mount(hdfsMount, hdfsPath);
      System.out.println("done");

      // Make sure output file does not exist.
      if (fileSystem.exists(outputPath)) {
        System.out.print("deleting " + outputPath + " ... ");
        fileSystem.delete(outputPath);
        System.out.println("done");
      }

      // Open the input stream.
      System.out.print("opening " + inputPath + " ... ");
      FileInStream is = fileSystem.openFile(inputPath);
      System.out.println("done");

      // Open the output stream, setting the write type to make sure result is persisted.
      System.out.print("opening " + outputPath + " ... ");
      CreateFilePOptions options = CreateFilePOptions.newBuilder()
          .setWriteType(WritePType.CACHE_THROUGH).setRecursive(true).build();
      FileOutStream os = fileSystem.createFile(outputPath, options);
      System.out.println("done");

      // Copy the data
      System.out.print("transferring data from " + inputPath + " to " + outputPath + " ... ");
      IOUtils.copy(is, os);
      System.out.println("done");

      // Close the input stream.
      System.out.print("closing " + inputPath + " ... ");
      is.close();
      System.out.println("done");

      // Close the output stream.
      System.out.print("closing " + outputPath + " ... ");
      os.close();
      System.out.println("done");
    } catch (Exception e) {
      System.out.println("fail");
      e.printStackTrace();
    } finally {
      // Make sure the S3 mount point is removed.
      try {
        if (fileSystem.exists(s3Mount)) {
          System.out.print("unmounting " + s3Mount + " ... ");
          fileSystem.unmount(s3Mount);
          System.out.println("done");
        }
      } catch (Exception e) {
        System.out.println("fail");
        e.printStackTrace();
      }

      // Make sure the HDFS mount point is removed.
      try {
        if (fileSystem.exists(hdfsMount)) {
          System.out.print("unmounting " + hdfsMount + " ... ");
          fileSystem.unmount(hdfsMount);
          System.out.println("done");
        }
      } catch (Exception e) {
        System.out.println("fail");
        e.printStackTrace();
      }
    }
  }
}
