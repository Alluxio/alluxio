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

package alluxio.fuse.ufs;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.UfsFileSystemOptions;
import alluxio.client.file.ufs.UfsBaseFileSystem;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.jnifuse.LibFuse;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.underfs.local.LocalUnderFileSystemFactory;
import alluxio.underfs.s3a.S3AUnderFileSystemFactory;
import alluxio.util.io.FileUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

/**
 * The abstract test for testing {@link alluxio.fuse.AlluxioJniFuseFileSystem}
 * with local UFS.
 */
@RunWith(Parameterized.class)
public abstract class AbstractTest {
  protected static final String FILE = "/file";
  protected static final String DIR = "/dir";
  protected static final String EXCEED_LENGTH_PATH_NAME
      = "/path" + String.join("", Collections.nCopies(16, "0123456789ABCDEF"));
  protected static final int DEFAULT_FILE_LEN = 64;
  protected static final Mode DEFAULT_MODE = new Mode(
      Mode.Bits.ALL, Mode.Bits.READ, Mode.Bits.READ);
  private static final String TEST_S3A_PATH_CONF = "alluxio.test.s3a.path";

  protected InstancedConfiguration mConf;
  protected AlluxioURI mRootUfs;
  protected FileSystem mFileSystem;
  protected FileSystemContext mContext;
  protected UfsFileSystemOptions mUfsOptions;
  protected boolean mIsLocalUFS;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {false, false},
        {true, false},
        {false, true},
        {true, true}
    });
  }

  /**
   * Runs {@link AbstractTest} with different configuration combinations.
   *
   * @param localDataCacheEnabled whether local data cache is enabled
   * @param localMetadataCacheEnabled whether local metadata cache is enabled
   */
  public AbstractTest(boolean localDataCacheEnabled, boolean localMetadataCacheEnabled) {
    mConf = Configuration.copyGlobal();
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ENABLED,
        PropertyKey.USER_CLIENT_CACHE_ENABLED.formatValue(localDataCacheEnabled), Source.RUNTIME);
    mConf.set(PropertyKey.USER_METADATA_CACHE_MAX_SIZE,
        PropertyKey.USER_METADATA_CACHE_MAX_SIZE.formatValue(localMetadataCacheEnabled ? 20000 : 0),
        Source.RUNTIME);
  }

  @Before
  public void before() throws Exception {
    String s3Path = System.getProperty(TEST_S3A_PATH_CONF);
    String ufs;
    if (s3Path != null) { // test against S3
      ufs = new AlluxioURI(s3Path).join(UUID.randomUUID().toString()).toString();
      UnderFileSystemFactoryRegistry.register(new S3AUnderFileSystemFactory());
    } else { // test against local
      ufs = AlluxioTestDirectory.createTemporaryDirectory("ufs").toString();
      UnderFileSystemFactoryRegistry.register(new LocalUnderFileSystemFactory());
      mIsLocalUFS = true;
    }
    mRootUfs = new AlluxioURI(ufs);
    mConf.set(PropertyKey.FUSE_MOUNT_POINT, "/t/mountPoint", Source.RUNTIME);
    mContext = FileSystemContext.create(ClientContext.create(mConf));
    LibFuse.loadLibrary(AlluxioFuseUtils.getLibfuseVersion(Configuration.global()));
    mUfsOptions = new UfsFileSystemOptions(ufs);
    mFileSystem = new UfsBaseFileSystem(mContext, mUfsOptions);
    beforeActions();
  }

  @After
  public void after() throws IOException {
    FileUtils.deletePathRecursively(mRootUfs.toString());
    afterActions();
  }

  /**
   * Add extra before actions.
   */
  public abstract void beforeActions();

  /**
   * Add extra after actions.
   */
  public abstract void afterActions() throws IOException;
}
