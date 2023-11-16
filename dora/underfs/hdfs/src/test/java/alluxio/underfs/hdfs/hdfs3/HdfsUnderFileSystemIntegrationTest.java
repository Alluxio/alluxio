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

import static junit.framework.Assert.assertFalse;
import static junit.framework.TestCase.assertEquals;

import alluxio.underfs.UfsStatus;
import alluxio.underfs.options.ListOptions;

import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
  public void testSetAndGetXAttribute() throws Exception {
    // create empty file
    String testFilePath = "/empty_file";
    OutputStream os = mUfs.create(testFilePath, getCreateOption());
    os.close();
    assertEquals(0, mUfs.getStatus(testFilePath).asUfsFileStatus().getContentLength());

    try {
      // Set attribute with a normal pair of key and value
      String attrKey = "key1";
      String attrValue = "value1";
      mUfs.setAttribute(testFilePath, attrKey, attrValue.getBytes());

      // Set attribute with an empty value
      String attrKey2 = "key2";
      String attrEmptyValue = "";
      mUfs.setAttribute(testFilePath, attrKey2, attrEmptyValue.getBytes());

      // Set attribute with an empty key
      String attrEmptyKey = "";
      mUfs.setAttribute(testFilePath, attrEmptyKey, attrValue.getBytes());

      Map<String, String> attrMap = mUfs.getAttributes(testFilePath);
      assertEquals(attrMap.size(), 2);
      assertEquals(attrMap.get(attrKey), attrValue);
      assertEquals(attrMap.get(attrKey2), attrEmptyValue);
      assertFalse(attrMap.containsKey(attrEmptyKey));
    } finally {
      mUfs.deleteFile(testFilePath);
    }
  }

  @Test
  public void testSetDuplicatedKeyToXAttr() throws Exception {
    // create empty file
    String testFilePath = "/dup_xattr_file";
    OutputStream os = mUfs.create(testFilePath, getCreateOption());
    os.close();
    assertEquals(0, mUfs.getStatus(testFilePath).asUfsFileStatus().getContentLength());

    try {
      // Set attribute with a same key twice to overwrite it
      String attrKey = "key1";
      String attrValue1 = "value1";
      String attrValue2 = "value2";
      mUfs.setAttribute(testFilePath, attrKey, attrValue1.getBytes());
      mUfs.setAttribute(testFilePath, attrKey, attrValue2.getBytes());
      Map<String, String> attrMap = mUfs.getAttributes(testFilePath);
      assertEquals(attrMap.size(), 1);
      assertEquals(attrMap.get(attrKey), attrValue2);
    } finally {
      mUfs.deleteFile(testFilePath);
    }
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

    String ufsSchema = mUfs.getFs().getUri().toString();

    testListPath(ufsSchema + "/testRoot");
    testListPath(ufsSchema + "/testRoot/");
    testListPath(ufsSchema + "/testRoot/testDirectory2");
    testListPath(ufsSchema + "/testRoot/testDirectory2/");
    testListPath(ufsSchema + "/testRoot/testDirectory2/testFileB");
    testListPath(ufsSchema + "/");

    testListPath("/testRoot");
    testListPath("/testRoot/");
    testListPath("/testRoot/testDirectory2");
    testListPath("/testRoot/testDirectory2/");
    testListPath("/testRoot/testDirectory2/testFileB");
    testListPath("/");

    Iterator<UfsStatus> iterator = mUfs.listStatusIterable("/testRoot",
        ListOptions.defaults(), null, 1000);

    List<UfsStatus> iteratorListRes = new ArrayList<>();
    while (iterator.hasNext()) {
      UfsStatus ufsStatus = iterator.next();
      iteratorListRes.add(ufsStatus);
    }

    assertEquals(8, iteratorListRes.size());
    assertEquals("testDirectory1", iteratorListRes.get(0).getName());
    assertEquals("testDirectory2", iteratorListRes.get(1).getName());
    assertEquals("testFileA", iteratorListRes.get(2).getName());
    assertEquals("testFileC", iteratorListRes.get(3).getName());
    assertEquals("testDirectory1/testFileB", iteratorListRes.get(4).getName());
    assertEquals("testDirectory2/testDirectory3", iteratorListRes.get(5).getName());
    assertEquals("testDirectory2/testFileD", iteratorListRes.get(6).getName());
    assertEquals("testDirectory2/testDirectory3/testFileE", iteratorListRes.get(7).getName());
  }

  private void testListPath(String path) throws IOException {
    Iterator<UfsStatus> iterator = mUfs.listStatusIterable(path,
        ListOptions.defaults(), null, 1000);

    List<UfsStatus> fullListRes = null;
    UfsStatus[] fullListResArray = mUfs.listStatus(path,
        ListOptions.defaults().setRecursive(true));
    if (fullListResArray == null) {
      fullListRes = Collections.emptyList();
    } else {
      fullListRes = Arrays.asList(fullListResArray);
    }

    List<UfsStatus> iteratorListRes = new ArrayList<>();
    while (iterator.hasNext()) {
      UfsStatus ufsStatus = iterator.next();
      iteratorListRes.add(ufsStatus);
    }

    assertEquals(fullListRes.size(), iteratorListRes.size());
    for (int i = 0; i < fullListRes.size(); i++) {
      UfsStatus fullListUfsStatus = fullListRes.get(i);
      UfsStatus iteratorListUfsStatus = iteratorListRes.get(i);
      assertEquals(fullListUfsStatus.getName(), iteratorListUfsStatus.getName());
      assertEquals(fullListUfsStatus.getGroup(), iteratorListUfsStatus.getGroup());
      assertEquals(fullListUfsStatus.getOwner(), iteratorListUfsStatus.getOwner());
      assertEquals(fullListUfsStatus.getMode(), iteratorListUfsStatus.getMode());
      assertEquals(fullListUfsStatus.isDirectory(), iteratorListUfsStatus.isDirectory());
      assertEquals(fullListUfsStatus.isFile(), iteratorListUfsStatus.isFile());
    }
  }
}
