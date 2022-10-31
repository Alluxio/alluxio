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

package alluxio.client.cli.fs.command;

import alluxio.AlluxioTestDirectory;
import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.worker.block.BlockStoreType;
import alluxio.worker.block.BlockWorker;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Tests for freeWorker Command.
 */
@RunWith(Parameterized.class)
public final class FreeWorkerCommandIntegrationTest extends BaseIntegrationTest {

  private static final int BLOCK_SIZE = Constants.MB;
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
            {BlockStoreType.PAGE},
            {BlockStoreType.FILE}
    });
  }

  public FreeWorkerCommandIntegrationTest(BlockStoreType blockStoreType) throws Exception{
    LocalAlluxioClusterResource.Builder builder = new LocalAlluxioClusterResource.Builder()
            .setProperty(PropertyKey.WORKER_BLOCK_STORE_TYPE, blockStoreType)
            .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE);

    if (blockStoreType == BlockStoreType.PAGE) {
      builder.setProperty(PropertyKey.USER_SHORT_CIRCUIT_ENABLED, false)
             .setProperty(PropertyKey.USER_STREAMING_READER_CHUNK_SIZE_BYTES, Constants.KB)
             .setProperty(PropertyKey.WORKER_PAGE_STORE_SIZES, ImmutableList.of(100 * Constants.KB))
             .setProperty(PropertyKey.WORKER_PAGE_STORE_DIRS,
                     ImmutableList.of(AlluxioTestDirectory.ALLUXIO_TEST_DIRECTORY));
    }
    mLocalAlluxioClusterResource = builder.build();
    mLocalAlluxioClusterResource.start();
  }

  @Test
  public void freeWorkerTest() throws IOException {
    List<String> paths = new ArrayList<>();
    if (Configuration.global().get(PropertyKey.WORKER_BLOCK_STORE_TYPE) == BlockStoreType.FILE) {
      int tierCount = Configuration.global().getInt(PropertyKey.WORKER_TIERED_STORE_LEVELS);
      for (int i = 0; i < tierCount; i++) {
        paths.addAll(Configuration.global().getList(PropertyKey
                .Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(i)));
      }
    } else {
      paths.addAll(Configuration.global().getList(PropertyKey.WORKER_PAGE_STORE_DIRS));
    }

    assertNotEquals(0, paths.size());

    for (String tmpPath : paths)  {
      String dataString = "freeWorkerCommandTest";
      File testFile = new File(tmpPath + "/testForFreeWorker.txt");
      testFile.createNewFile();
      FileOutputStream fos = new FileOutputStream(testFile);
      fos.write(dataString.getBytes());
      fos.close();
    }

    mLocalAlluxioClusterResource.get().getWorkerProcess().getWorker(BlockWorker.class).freeWorker();

    int subFileCount = 0;
    for (String tmpPath : paths)  {
      File[] files = new File(tmpPath).listFiles();
      subFileCount += files.length;
    }

    assertEquals(0, subFileCount);
  }
}
