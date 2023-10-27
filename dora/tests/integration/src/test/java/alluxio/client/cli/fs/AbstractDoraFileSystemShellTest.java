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

package alluxio.client.cli.fs;

import alluxio.AlluxioURI;
import alluxio.cli.fs.FileSystemShell;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.Configuration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioCluster;
import alluxio.util.io.BufferUtils;

import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * The base class for all the {@link FileSystemShell} test classes.
 */
public abstract class AbstractDoraFileSystemShellTest extends AbstractDoraShellIntegrationTest {
  public LocalAlluxioCluster mLocalAlluxioCluster;
  public FileSystem mFileSystem;
  public FileSystemShell mFsShell;

  @Before
  public void before() throws Exception {
    super.before();
    mLocalAlluxioClusterResource.start();
    mLocalAlluxioCluster = mLocalAlluxioClusterResource.get();
    mFileSystem = mLocalAlluxioCluster.getClient();
    mFsShell = new FileSystemShell(Configuration.global());
  }

  public AbstractDoraFileSystemShellTest(int numWorkers) throws IOException {
    super(numWorkers);
  }

  protected void createByteFileInUfs(String fileName, int length) throws IOException {
    if (fileName.startsWith("/")) {
      fileName = fileName.substring(1);
    }
    File f = mTestFolder.newFile(fileName);
    Files.write(f.toPath(), BufferUtils.getIncreasingByteArray(length));
  }

  protected void createByteFileInAlluxio(String filename, byte[] bytes, WritePType writePType)
      throws IOException, AlluxioException {
    FileOutStream fos = mFileSystem.createFile(new AlluxioURI(filename),
        CreateFilePOptions.newBuilder().setWriteType(writePType).setRecursive(true).build());
    fos.write(bytes);
    fos.close();
  }
}
