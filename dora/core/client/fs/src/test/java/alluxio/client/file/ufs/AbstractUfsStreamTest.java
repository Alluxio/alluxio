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

package alluxio.client.file.ufs;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.FileSystemOptions;
import alluxio.client.file.options.UfsFileSystemOptions;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.DeletePOptions;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.underfs.local.LocalUnderFileSystemFactory;

import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.UUID;

/**
 * Add unit tests for streams of {@link UfsBaseFileSystem}.
 */
public abstract class AbstractUfsStreamTest {
  protected static final int CHUNK_SIZE = 100;
  protected InstancedConfiguration mConf;
  protected AlluxioURI mRootUfs;
  protected FileSystem mFileSystem;

  /**
   * Runs {@link AbstractUfsStreamTest} with different configuration combinations.
   */
  public AbstractUfsStreamTest() {
    mConf = Configuration.copyGlobal();
  }

  /**
   * Sets up the file system and the context before a test runs.
   */
  @Before
  public void before() {
    String ufs = AlluxioTestDirectory.createTemporaryDirectory("ufsInStream").toString();
    mRootUfs = new AlluxioURI(ufs);
    UnderFileSystemFactoryRegistry.register(new LocalUnderFileSystemFactory());
    final FileSystemOptions fileSystemOptions =
        new FileSystemOptions.Builder()
            .setUfsFileSystemOptions(new UfsFileSystemOptions(ufs))
            .setDoraCacheEnabled(false)
            .setDataCacheEnabled(false)
            .build();
    mFileSystem = FileSystem.Factory.create(FileSystemContext.create(
        ClientContext.create(mConf)), fileSystemOptions);
  }

  @After
  public void after() throws IOException, AlluxioException {
    for (URIStatus status : mFileSystem.listStatus(mRootUfs)) {
      mFileSystem.delete(new AlluxioURI(status.getPath()),
          DeletePOptions.newBuilder().setRecursive(true).build());
    }
  }

  protected AlluxioURI getUfsPath() {
    return mRootUfs.join("streamTest" + UUID.randomUUID());
  }
}
