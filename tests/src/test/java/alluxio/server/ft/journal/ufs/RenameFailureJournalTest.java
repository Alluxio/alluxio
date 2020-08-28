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

package alluxio.server.ft.journal.ufs;

import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.UnderFileSystemFactoryRegistryRule;
import alluxio.client.file.FileSystem;
import alluxio.conf.ServerConfiguration;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.testutils.underfs.delegating.DelegatingUnderFileSystem;
import alluxio.testutils.underfs.delegating.DelegatingUnderFileSystemFactory;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystem.Factory;
import alluxio.util.CommonUtils;

import com.google.common.io.Files;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Integration test where the rename operation often fails. Makes sure the journal can recover
 * from this.
 */
public class RenameFailureJournalTest {
  private static final String LOCAL_UFS_PATH = Files.createTempDir().getAbsolutePath();

  private FileSystem mFs;

  // An under file system which fails 90% of its renames.
  private static final UnderFileSystem UFS =
      new DelegatingUnderFileSystem(Factory.create(LOCAL_UFS_PATH, ServerConfiguration.global())) {
        @Override
        public boolean renameFile(String src, String dst) throws IOException {
          if (ThreadLocalRandom.current().nextInt(10) == 0) {
            return mUfs.renameFile(src, dst);
          } else {
            return false;
          }
        }
      };

  @ClassRule
  public static UnderFileSystemFactoryRegistryRule sUnderfilesystemfactoryregistry =
      new UnderFileSystemFactoryRegistryRule(new DelegatingUnderFileSystemFactory(UFS));

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.MASTER_JOURNAL_FOLDER,
              DelegatingUnderFileSystemFactory.DELEGATING_SCHEME + "://" + LOCAL_UFS_PATH)
          .setProperty(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, Integer.toString(128))
          .setProperty(PropertyKey.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS, 0)
          .setProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
          .setProperty(PropertyKey.MASTER_JOURNAL_FLUSH_RETRY_INTERVAL, "0")
          .build();

  @Before
  public void before() throws Exception {
    mFs = FileSystem.Factory.create(ServerConfiguration.global());
  }

  @Test
  public void testMultiRestart() throws Exception {
    List<AlluxioURI> paths = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      AlluxioURI path = new AlluxioURI("/path" + i);
      paths.add(path);
      mFs.createDirectory(path);
      CommonUtils.sleepMs(10);
    }
    for (int i = 0; i < 3; i++) {
      mLocalAlluxioClusterResource.get().restartMasters();
      for (AlluxioURI path : paths) {
        assertTrue(mFs.exists(path));
      }
    }
  }
}
