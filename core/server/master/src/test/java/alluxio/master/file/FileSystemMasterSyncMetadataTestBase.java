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

package alluxio.master.file;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalTestUtils;
import alluxio.master.journal.JournalType;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.metrics.MetricsSystem;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.user.UserState;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.local.LocalUnderFileSystem;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.PathUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileSystemMasterSyncMetadataTestBase {
  protected static final AlluxioURI ROOT = new AlluxioURI("/");
  protected static final String TEST_DIR_PREFIX = "/dir";
  protected static final String TEST_FILE_PREFIX = "/file";
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();
  protected String mUfsUri;
  protected FlakyLocalUnderFileSystem mUfs;
  protected ExecutorService mFileSystemExecutorService;
  protected ExecutorService mUfsStateCacheExecutorService;
  protected MasterRegistry mRegistry;
  protected DefaultFileSystemMaster mFileSystemMaster;

  protected InodeTree mInodeTree;

  protected static UfsStatus createUfsStatusWithName(String name) {
    return new UfsFileStatus(name, "hash", 0, 0L, "owner", "group", (short) 0, null, 0);
  }

  @Before
  public void before() throws Exception {
    UserState us = UserState.Factory.create(Configuration.global());
    AuthenticatedClientUser.set(us.getUser().getName());

    mTempDir.create();
    mUfsUri = mTempDir.newFolder().getAbsolutePath();

    mUfs = new FlakyLocalUnderFileSystem(new AlluxioURI(mUfsUri),
        UnderFileSystemConfiguration.defaults(Configuration.global()));
    PowerMockito.mockStatic(UnderFileSystem.Factory.class);
    Mockito.when(UnderFileSystem.Factory.create(anyString(), any())).thenReturn(mUfs);

    Configuration.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS);
    Configuration.set(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, mUfsUri);
    String journalFolderUri = mTempDir.newFolder().getAbsolutePath();

    mFileSystemExecutorService = Executors
        .newFixedThreadPool(4, ThreadFactoryUtils.build("FileSystemMaster-%d", true));
    mUfsStateCacheExecutorService = Executors
        .newFixedThreadPool(4, ThreadFactoryUtils.build("UfsStateCache-%d", true));
    mRegistry = new MasterRegistry();
    JournalSystem journalSystem =
        JournalTestUtils.createJournalSystem(journalFolderUri);
    CoreMasterContext context = MasterTestUtils.testMasterContext(journalSystem);
    new MetricsMasterFactory().create(mRegistry, context);
    BlockMaster blockMaster = new BlockMasterFactory().create(mRegistry, context);
    mFileSystemMaster = new DefaultFileSystemMaster(blockMaster, context,
        ExecutorServiceFactories.constantExecutorServiceFactory(mFileSystemExecutorService),
        Clock.systemUTC());
    mInodeTree = mFileSystemMaster.getInodeTree();
    mRegistry.add(FileSystemMaster.class, mFileSystemMaster);
    journalSystem.start();
    journalSystem.gainPrimacy();
    mRegistry.start(true);

    MetricsSystem.resetAllMetrics();
  }

  @After
  public void after() throws Exception {
    mRegistry.stop();
    mFileSystemExecutorService.shutdown();
    mUfsStateCacheExecutorService.shutdown();
  }

  protected void createUfsDir(String path) throws IOException {
    mUfs.mkdirs(PathUtils.concatPath(mUfsUri, path));
  }

  protected OutputStream createUfsFile(String path) throws IOException {
    return mUfs.create(PathUtils.concatPath(mUfsUri, path));
  }

  protected static class FlakyLocalUnderFileSystem extends LocalUnderFileSystem {
    public boolean mThrowIOException = false;
    public boolean mThrowRuntimeException = false;
    public boolean mIsSlow = false;
    public long mSlowTimeMs = 2000L;

    public FlakyLocalUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration conf) {
      super(uri, conf);
    }

    @Override
    public UfsStatus getStatus(String path) throws IOException {
      if (mThrowRuntimeException) {
        throw new RuntimeException();
      }
      if (mThrowIOException) {
        throw new IOException();
      }
      if (mIsSlow) {
        try {
          Thread.sleep(mSlowTimeMs);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      return super.getStatus(path);
    }

    @Override
    public UfsStatus[] listStatus(String path) throws IOException {
      if (mThrowRuntimeException) {
        throw new RuntimeException();
      }
      if (mThrowIOException) {
        throw new IOException();
      }
      if (mIsSlow) {
        try {
          Thread.sleep(mSlowTimeMs);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      return super.listStatus(path);
    }
  }
}

