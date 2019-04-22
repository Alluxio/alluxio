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
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ListStatusPOptions;

import java.io.IOException;

/**
 * Profiler client for Alluxio.
 */
public class AlluxioProfilerClient extends ProfilerClient {


  private final FileSystem mClient;

  public AlluxioProfilerClient() {
    mClient = FileSystem.Factory.get();
  }

  @Override
  public void delete(String rawPath) throws IOException {
    AlluxioURI path = new AlluxioURI(rawPath);
    try {
      mClient.delete(path,
          DeletePOptions.newBuilder().setRecursive(true).setAlluxioOnly(false).build());
    } catch (FileDoesNotExistException e) {
      // ok
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void createFile(String rawPath, long fileSize) throws IOException {
    try (FileOutStream stream = mClient.createFile(new AlluxioURI(rawPath),
        CreateFilePOptions.newBuilder().build())) {
      writeOutput(stream, fileSize);
    } catch (FileDoesNotExistException e) {
      // ok
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void createDir(String rawPath) throws IOException {
    try {
      mClient.createDirectory(new AlluxioURI(rawPath),
          CreateDirectoryPOptions.newBuilder().setRecursive(true).setAllowExists(true).build());
    } catch (FileDoesNotExistException e) {
      // ok
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void list(String rawPath) throws IOException {
    try {
      mClient.listStatus(new AlluxioURI(rawPath), ListStatusPOptions.newBuilder()
          .setRecursive(true)
          .build());
    } catch (FileDoesNotExistException e) {
      // ok
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void read(String rawPath) throws IOException {
    try (FileInStream fis = mClient.openFile(new AlluxioURI(rawPath))) {
      readInput(fis);
    } catch (FileDoesNotExistException e) {
      // ok
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }
}
