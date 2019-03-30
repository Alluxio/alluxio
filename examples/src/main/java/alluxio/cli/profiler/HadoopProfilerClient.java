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

package alluxio.cli.profiler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HadoopProfilerClient extends ProfilerClient {

  private final FileSystem mClient;

  public HadoopProfilerClient(String hadoopUri, Configuration conf) {
    try {
      mClient = FileSystem.get(new URI(hadoopUri), conf);
    } catch (URISyntaxException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Delete an inode at the given path
   *
   * @param rawPath directory to clean
   * @throws IOException
   */
  @Override
  public void createFile(String rawPath, long fileSize) throws IOException {
    Path filePath = new Path(rawPath);
    if (!sDryRun) {
      try (FSDataOutputStream stream = mClient.create(filePath)) {
        writeOutput(stream, fileSize);
      }
    } else {
      System.out.println("create: " + filePath);
    }
  }

  /**
   * Delete an inode at the given path
   *
   * @param rawPath directory to clean
   * @throws IOException
   */
  @Override
  public void createDir(String rawPath) throws IOException {
    if (!sDryRun) {
      mClient.mkdirs(new Path(rawPath));
    } else {
      System.out.println("create: " + rawPath);
    }
  }

  /**
   * Delete an inode at the given path
   *
   * @param rawPath directory to clean
   * @throws IOException
   */
  @Override
  public void delete(String rawPath) throws IOException {
    if (!sDryRun) {
      mClient.delete(new Path(rawPath), true);
    } else {
      System.out.println("delete: " + rawPath);
    }
  }

  @Override
  public void list(String rawPath) throws IOException {
    if (!sDryRun) {
      mClient.listStatus(new Path(rawPath));
    } else {
      System.out.println("listStatus: " + rawPath);
    }
  }
}