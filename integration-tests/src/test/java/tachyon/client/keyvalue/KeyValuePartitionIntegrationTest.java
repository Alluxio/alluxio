/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.keyvalue;

import java.io.IOException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.file.TachyonFileSystem;
import tachyon.exception.TachyonException;
import tachyon.util.io.PathUtils;

/**
 * Integration tests for {@link KeyValuePartitionReader} and {@link KeyValuePartitionWriter}.
 */
public final class KeyValuePartitionIntegrationTest {
  private static final int BLOCK_SIZE = 512 * Constants.MB;
  private static final byte[] KEY1 = "key1".getBytes();
  private static final byte[] KEY2 = "key2_foo".getBytes();
  private static final byte[] VALUE1 = "value1".getBytes();
  private static final byte[] VALUE2 = "value2_bar".getBytes();
  private static TachyonFileSystem sTfs;
  private KeyValuePartitionWriter mKeyValuePartitionWriter;
  private KeyValuePartitionReader mKeyValuePartitionReader;

  @ClassRule
  public static LocalTachyonClusterResource sLocalTachyonClusterResource =
      new LocalTachyonClusterResource(Constants.GB, Constants.KB, BLOCK_SIZE,
          /* ensure key-value service is turned on */
          Constants.KEYVALUE_ENABLED, "true");

  @BeforeClass
  public static void beforeClass() throws Exception {
    sTfs = sLocalTachyonClusterResource.get().getClient();
  }

  @Test
  public void readerWriterTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    TachyonURI uri = new TachyonURI(uniqPath);
    mKeyValuePartitionWriter = KeyValuePartitionWriter.Factory.create(uri);
    mKeyValuePartitionWriter.put(KEY1, VALUE1);
    mKeyValuePartitionWriter.put(KEY2, VALUE2);
    mKeyValuePartitionWriter.close();
    // Expect the key-value file exists
    Assert.assertNotNull(sTfs.openIfExists(uri));
    mKeyValuePartitionReader = KeyValuePartitionReader.Factory.create(uri);
    Assert.assertArrayEquals(VALUE1, mKeyValuePartitionReader.get(KEY1));
    Assert.assertArrayEquals(VALUE2, mKeyValuePartitionReader.get(KEY2));
    Assert.assertNull(mKeyValuePartitionReader.get("NoSuchKey".getBytes()));
  }
}
