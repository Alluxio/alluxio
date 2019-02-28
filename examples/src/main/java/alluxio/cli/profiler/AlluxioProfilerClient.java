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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.util.io.PathUtils;

import java.io.IOException;
import java.util.Arrays;

/**
 * Profiler client for Alluxio.
 */
public class AlluxioProfilerClient extends ProfilerClient {


  private final FileSystem mClient;

  public AlluxioProfilerClient() {
    mClient = FileSystem.Factory.get();
  }

  @Override
  public void cleanup(String dir) throws IOException {
    try {
      AlluxioURI path = new AlluxioURI(dir);
      try {
        if (!sDryRun) {
          mClient.delete(path,
              DeletePOptions.newBuilder().setRecursive(true).setAlluxioOnly(false).build());
        } else {
          System.out.println("Delete: " + path);
        }
      } catch (FileDoesNotExistException e) {
        // ok if it doesn't exist already
      }

      if (!sDryRun) {
        mClient.createDirectory(path,
            CreateDirectoryPOptions.newBuilder().setRecursive(true).setAllowExists(true).build());
      } else {
        System.out.println("create directory: " + path);
      }
    } catch (AlluxioException e) {
      throw new IOException(e);
    }

    return;
  }

  @Override
  public void createFiles(String dir, long numFiles, long filesPerDir, long fileSize) throws IOException {
    int createdFiles = 0;
    while (createdFiles < numFiles) {
      try {
        String subDir = PathUtils.concatPath(dir, createdFiles);
        AlluxioURI subUri = new AlluxioURI(subDir);
        if (!sDryRun) {
          mClient.createDirectory(subUri,
              CreateDirectoryPOptions.newBuilder().setRecursive(true).setAllowExists(true).build());
        } else {
          System.out.println("Create directory: " + subUri);
        }
        for (int j = 0; j < filesPerDir; j++) {
          AlluxioURI filePath = new AlluxioURI(PathUtils.concatPath(subUri.getPath(), j));
          if (!sDryRun) {
            try (FileOutStream stream = mClient.createFile(filePath,
                CreateFilePOptions.newBuilder().build())) {
              writeOutput(stream, fileSize);
            }
          } else {
            System.out.println("Create file: " + filePath);
          }
          createdFiles++;
        }
      } catch (AlluxioException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
