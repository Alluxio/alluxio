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

import org.junit.Test;

import java.io.IOException;

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
}
