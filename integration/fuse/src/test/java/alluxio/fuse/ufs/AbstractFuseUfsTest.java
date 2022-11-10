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
import alluxio.client.file.ufs.UfsBaseFileSystem;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.fuse.AlluxioJniFuseFileSystem;
import alluxio.jnifuse.LibFuse;
import alluxio.jnifuse.struct.FileStat;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.underfs.local.LocalUnderFileSystemFactory;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.FileUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

public abstract class AbstractFuseUfsTest {
  private static final String MOUNT_POINT = "/t/mountPoint";
  private static final String FILE = "/file";
  private static final String DIR = "/dir";
  private static final String EXCEED_LENGTH_PATH_NAME
      = "/path" + String.join("", Collections.nCopies(16, "0123456789ABCDEF"));
  private AlluxioURI mRootUfs;
  private FileSystem mFileSystem;
  private AlluxioJniFuseFileSystem mFuseFs;
  private FileStat mFileStat;

  @Before
  public void before() throws Exception {
    InstancedConfiguration conf = Configuration.copyGlobal();
    String ufs = AlluxioTestDirectory.createTemporaryDirectory("ufs").toString();
    conf.set(PropertyKey.FUSE_MOUNT_POINT, MOUNT_POINT, Source.RUNTIME);
    conf.set(PropertyKey.USER_UFS_ENABLED, true, Source.RUNTIME);
    conf.set(PropertyKey.USER_ROOT_UFS, ufs, Source.RUNTIME);
    configure();
    mRootUfs = new AlluxioURI(ufs);
    LocalUnderFileSystemFactory localUnderFileSystemFactory = new LocalUnderFileSystemFactory();
    UnderFileSystemFactoryRegistry.register(localUnderFileSystemFactory);
    FileSystemContext context = FileSystemContext.create(
        ClientContext.create(conf));
    LibFuse.loadLibrary(AlluxioFuseUtils.getLibfuseVersion(Configuration.global()));
    mFileSystem = new UfsBaseFileSystem(context);
    mFuseFs = new AlluxioJniFuseFileSystem(context, mFileSystem);
    mFileStat = FileStat.of(ByteBuffer.allocateDirect(256));
    beforeActions();
  }

  /**
   * Overwrites the test configuration in this method.
   */
  public abstract void configure();

  /**
   * Add extra before actions.
   */
  public abstract void beforeActions();

  /**
   * Add extra after actions.
   */
  public abstract void afterActions() throws IOException;

  @After
  public void after() throws IOException {
    afterActions();
    FileUtils.deletePathRecursively(mRootUfs.toString());
    BufferUtils.cleanDirectBuffer(mFileStat.getBuffer());
  }
}
