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

package alluxio.underfs.hdfs.hdfs3;

import static junit.framework.TestCase.assertEquals;

import alluxio.underfs.UfsStatus;
import alluxio.underfs.options.ListOptions;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HdfsUnderFileSystemIntegrationTest extends HdfsUnderFileSystemIntegrationTestBase {
  @Test
  public void testWriteEmptyFile() throws Exception {
    writeEmptyFileTest();
  }

  @Test
  public void testWriteMultiBlockFileTest() throws Exception {
    writeMultiBlockFileTest("/test_file");
  }

  @Test(expected = IOException.class)
  public void testException() throws Exception {
    hdfsDownDuringUploadTest();
  }

  @Test
  public void testListUfsStatusIterator() throws Exception {
    /**
     * The mock hierarchy looks like:
     * /testRoot
     *   |- testDirectory1
     *        |- testFileB
     *   |- testDirectory2
     *        |- testDirectory3
     *             |- testFileE
     *        |- testFileD
     *   |- testFileA
     *   |- testFileC
     */
    createDirectoryTest("/testRoot");
    createDirectoryTest("/testRoot/testDirectory1");
    createDirectoryTest("/testRoot/testDirectory2");
    createDirectoryTest("/testRoot/testDirectory2/testDirectory3");
    writeMultiBlockFileTest("/testRoot/testFileA");
    writeMultiBlockFileTest("/testRoot/testDirectory1/testFileB");
    writeMultiBlockFileTest("/testRoot/testFileC");
    writeMultiBlockFileTest("/testRoot/testDirectory2/testFileD");
    writeMultiBlockFileTest("/testRoot/testDirectory2/testDirectory3/testFileE");

    Iterator<UfsStatus> iterator = mUfs.listStatusIterable("/testRoot",
        ListOptions.defaults(), null, 1000);

    List<UfsStatus> listResult = new ArrayList<>();
    while (iterator.hasNext()) {
      UfsStatus ufsStatus = iterator.next();
      listResult.add(ufsStatus);
    }
    assertEquals(8, listResult.size());
    assertEquals("testDirectory1", listResult.get(0).getName());
    assertEquals("testDirectory2", listResult.get(1).getName());
    assertEquals("testFileA", listResult.get(2).getName());
    assertEquals("testFileC", listResult.get(3).getName());
    assertEquals("testFileB", listResult.get(4).getName());
    assertEquals("testDirectory3", listResult.get(5).getName());
    assertEquals("testFileD", listResult.get(6).getName());
    assertEquals("testFileE", listResult.get(7).getName());
  }
}
