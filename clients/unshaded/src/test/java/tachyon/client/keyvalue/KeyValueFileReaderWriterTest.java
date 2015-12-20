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

import org.junit.Assert;
import org.junit.Test;

import tachyon.client.file.ByteArrayOutStream;
import tachyon.worker.keyvalue.Index;
import tachyon.worker.keyvalue.RandomAccessPayloadReader;

import java.nio.ByteBuffer;

/**
 * unit tests of {@link OutStreamKeyValueFileWriter} and {@link RandomAccessKeyValueFileReader}
 */
public class KeyValueFileReaderWriterTest {
  private static final byte[] KEY1 = "key1".getBytes();
  private static final byte[] KEY2 = "key2_foo".getBytes();
  private static final byte[] VALUE1 = "value1".getBytes();
  private static final byte[] VALUE2 = "value2_bar".getBytes();

  private ByteArrayOutStream mOutStream = new ByteArrayOutStream();
  private KeyValueFileWriter mWriter = new OutStreamKeyValueFileWriter(mOutStream);
  private KeyValueFileReader mReader;

  @Test
  public void putTest() throws Exception {
    mWriter.put(KEY1, VALUE1);
  }

  @Test
  public void closeTest() throws Exception {
    mWriter.close();
  }

  @Test
  public void buildAndLoadTest() throws Exception {
    mWriter.put(KEY1, VALUE1);
    mWriter.put(KEY2, VALUE2);
    mWriter.build();
    byte[] fileData = mOutStream.toByteArray();
    mReader = new RandomAccessKeyValueFileReader(ByteBuffer.wrap(fileData));
    Assert.assertArrayEquals(VALUE1, mReader.get(KEY1));
    Assert.assertArrayEquals(VALUE2, mReader.get(KEY2));

    Assert.assertNull(mReader.get("NoSuchKey".getBytes()));
  }

  @Test
  public void keyCountTest() throws Exception {
    Assert.assertEquals(0, mWriter.keyCount());
    mWriter.put(KEY1, VALUE1);
    Assert.assertEquals(1, mWriter.keyCount());
    mWriter.put(KEY2, VALUE2);
    Assert.assertEquals(2, mWriter.keyCount());
  }

  @Test
  public void byteCountTest() throws Exception {
    mWriter.put(KEY1, VALUE1);
    Assert.assertTrue(mWriter.byteCount() > 0);
  }
}
