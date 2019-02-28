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

import alluxio.util.io.PathUtils;

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

  @Override
  public void cleanup(String dir) throws IOException {
    if (!sDryRun) {
      mClient.delete(new Path(dir), true);
    } else {
      System.out.println("delete: " + dir);
    }

    if (!sDryRun) {
      mClient.mkdirs(new Path(dir));
    } else {
      System.out.println("create: " + dir);
    }
  }

  @Override
  public void createFiles(String dir, long numFiles, long filesPerDir, long fileSize)
      throws IOException {
    int createdFiles = 0;
    while (createdFiles < numFiles) {
      String subDir = PathUtils.concatPath(dir, createdFiles);
      if (!sDryRun) {
        mClient.mkdirs(new Path(subDir));

      } else {
        System.out.println("create: " + subDir);
      }
      for (int j = 0; j < filesPerDir; j++) {
        Path filePath = new Path(PathUtils.concatPath(subDir, j));
        if (!sDryRun) {
          try (FSDataOutputStream stream = mClient.create(filePath)) {
            writeOutput(stream, fileSize);
          }
        } else {
          System.out.println("create: " + filePath);
        }
        createdFiles++;
      }
    }
  }
}
