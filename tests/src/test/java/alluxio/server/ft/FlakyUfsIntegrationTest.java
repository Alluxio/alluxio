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

package alluxio.server.ft;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.UnderFileSystemFactoryRegistryRule;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.MasterClientContext;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.testutils.underfs.delegating.DelegatingUnderFileSystem;
import alluxio.testutils.underfs.delegating.DelegatingUnderFileSystemFactory;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.io.Files;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;

public final class FlakyUfsIntegrationTest extends BaseIntegrationTest {
  private static final String LOCAL_UFS_PATH = Files.createTempDir().getAbsolutePath();
  // An under file system which fails 90% of its renames.
  private static final UnderFileSystem UFS =
      new DelegatingUnderFileSystem(UnderFileSystem.Factory.create(LOCAL_UFS_PATH,
          ServerConfiguration.global())) {
        @Override
        public boolean deleteFile(String path) throws IOException {
          if (ThreadLocalRandom.current().nextBoolean()) {
            return mUfs.deleteFile(path);
          } else {
            return false;
          }
        }

        @Override
        public boolean deleteExistingFile(String path) throws IOException {
          if (ThreadLocalRandom.current().nextBoolean()) {
            return mUfs.deleteExistingFile(path);
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
          .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
              DelegatingUnderFileSystemFactory.DELEGATING_SCHEME + "://" + LOCAL_UFS_PATH).build();
  private FileSystem mFs;

  @Before
  public void before() throws Exception {
    mFs = mLocalAlluxioClusterResource.get().getClient();
  }

  @Test
  public void deletePartial() throws Exception {
    mFs.createDirectory(new AlluxioURI("/dir"));
    for (int i = 0; i < 100; i++) {
      FileSystemTestUtils.createByteFile(mFs, "/dir/test" + i, 100,
          CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build());
    }
    String ufs = LOCAL_UFS_PATH;
    // This will make the "/dir" directory out of sync so that the files are deleted individually.
    java.nio.file.Files.createDirectory(Paths.get(ufs, "/dir/testunknown"));
    try {
      mFs.delete(new AlluxioURI("/dir"), DeletePOptions.newBuilder().setRecursive(true).build());
      fail("Expected an exception to be thrown");
    } catch (AlluxioException e) {
      // expected
    }
    int deleted = 0;
    for (int i = 0; i < 100; i++) {
      if (!mFs.exists(new AlluxioURI("/dir/test" + i))) {
        deleted++;
      }
    }
    // It's a coin flip whether each delete succeeds. With extremely high likelihood, between 10 and
    // 90 deletes should succeed.
    assertThat(deleted, Matchers.greaterThan(10));
    assertThat(deleted, Matchers.lessThan(90));
    mFs.free(new AlluxioURI("/"), FreePOptions.newBuilder().setRecursive(true).build());
    BlockMasterClient blockClient =
        BlockMasterClient.Factory.create(MasterClientContext
            .newBuilder(ClientContext.create(ServerConfiguration.global())).build());
    CommonUtils.waitFor("data to be freed", () -> {
      try {
        return blockClient.getUsedBytes() == 0;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, WaitForOptions.defaults().setTimeoutMs(10 * Constants.SECOND_MS));
  }
}
