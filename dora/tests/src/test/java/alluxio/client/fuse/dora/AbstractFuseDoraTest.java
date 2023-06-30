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

package alluxio.client.fuse.dora;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.UfsFileSystemOptions;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.jnifuse.LibFuse;
import alluxio.security.authorization.Mode;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.FileUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

/**
 * The abstract test for testing {@link alluxio.fuse.AlluxioJniFuseFileSystem}
 * with local UFS.
 */
public abstract class AbstractFuseDoraTest {
  protected static final String EXCEED_LENGTH_PATH_NAME
      = "/path" + String.join("", Collections.nCopies(16, "0123456789ABCDEF"));
  protected static final int DEFAULT_FILE_LEN = 64;
  protected static final Mode DEFAULT_MODE = new Mode(
      Mode.Bits.ALL, Mode.Bits.READ, Mode.Bits.READ);
  protected static final AlluxioURI UFS_ROOT =
      new AlluxioURI(AlluxioTestDirectory.createTemporaryDirectory("ufs_root").getAbsolutePath());
  private static final String MOUNT_POINT = AlluxioTestDirectory
      .createTemporaryDirectory("fuse_mount").toString();

  private static final String PAGING_STORE_DIR = AlluxioTestDirectory
      .createTemporaryDirectory("ramdisk").toString();

  protected FileSystem mFileSystem;
  protected FileSystemContext mContext;
  protected UfsFileSystemOptions mUfsOptions;

  @Rule
  public LocalAlluxioClusterResource mClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_NETTY_DATA_TRANSMISSION_ENABLED, true)
          // TODO(lu) support dora client metadata cache in read-write tests
          .setProperty(PropertyKey.DORA_CLIENT_METADATA_CACHE_ENABLED, false)
          .setProperty(PropertyKey.DORA_CLIENT_UFS_ROOT, UFS_ROOT.toString())
          .setProperty(PropertyKey.MASTER_WORKER_REGISTER_LEASE_ENABLED, false)
          .setProperty(PropertyKey.USER_SHORT_CIRCUIT_ENABLED, false)
          .setProperty(PropertyKey.USER_STREAMING_READER_CHUNK_SIZE_BYTES, Constants.KB)
          .setProperty(PropertyKey.FUSE_MOUNT_POINT, MOUNT_POINT)
          .setProperty(PropertyKey.WORKER_BLOCK_STORE_TYPE, "PAGE")
          .setProperty(PropertyKey.WORKER_PAGE_STORE_TYPE, "LOCAL")
          .setProperty(PropertyKey.WORKER_PAGE_STORE_DIRS, PAGING_STORE_DIR)
          .setProperty(PropertyKey.WORKER_PAGE_STORE_SIZES, "10GB")
          .setProperty(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE, "1MB")
          .setProperty(PropertyKey.DORA_CLIENT_METADATA_CACHE_ENABLED, true)
          .build();

  @Before
  public void before() throws Exception {
    LibFuse.loadLibrary(AlluxioFuseUtils.getLibfuseVersion(Configuration.global()));
    mContext = FileSystemContext.create(ClientContext.create(Configuration.global()));
    mFileSystem = mClusterResource.get().getClient();
    mUfsOptions = new UfsFileSystemOptions(UFS_ROOT.toString());
    beforeActions();
  }

  @After
  public void after() throws IOException {
    File dir = new File(UFS_ROOT.toString());
    File[] files = dir.listFiles();
    if (files != null) {
      for (File file : files) {
        FileUtils.deletePathRecursively(file.getPath());
      }
    }
    afterActions();
  }

  /**
   * Add extra before actions.
   */
  public abstract void beforeActions() throws IOException;

  /**
   * Add extra after actions.
   */
  public abstract void afterActions() throws IOException;
}
