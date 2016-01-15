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

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.client.ByteArrayOutStream;

/**
 * Unit tests of {@link ByteBufferKeyValuePartitionReader}.
 */
public final class ByteBufferKeyValuePartitionReaderTest {
  private static final byte[] KEY1 = "key1".getBytes();
  private static final byte[] KEY2 = "key2_foo".getBytes();
  private static final byte[] VALUE1 = "value1".getBytes();
  private static final byte[] VALUE2 = "value2_bar".getBytes();
  private static ByteArrayOutStream sOutStream;
  private static BaseKeyValuePartitionWriter sWriter;
  private static ByteBuffer sBuffer;
  private ByteBufferKeyValuePartitionReader mReader;

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() throws Exception {
    sOutStream = new ByteArrayOutStream();
    sWriter = new BaseKeyValuePartitionWriter(sOutStream);
    sWriter.put(KEY1, VALUE1);
    sWriter.put(KEY2, VALUE2);
    sWriter.close();
    sBuffer = ByteBuffer.wrap(sOutStream.toByteArray());
  }

  @Before
  public void before() throws Exception {
    mReader = new ByteBufferKeyValuePartitionReader(sBuffer);
  }

  /**
   * Tests {@link ByteBufferKeyValuePartitionReader#get} can retrieve values stored before.
   */
  @Test
  public void getTest() throws Exception {
    Assert.assertArrayEquals(VALUE1, mReader.get(KEY1));
    Assert.assertArrayEquals(VALUE2, mReader.get(KEY2));
    Assert.assertNull(mReader.get("NoSuchKey".getBytes()));
    Assert.assertArrayEquals(VALUE1, mReader.get(KEY1));
    Assert.assertArrayEquals(VALUE2, mReader.get(KEY2));
  }

  /**
   * Tests {@link ByteBufferKeyValuePartitionReader#close} works.
   */
  @Test
  public void closeTest() throws Exception {
    mReader.close();
    // Expect close to be no-op
    mReader.close();
  }

  /**
   * Tests {@link ByteBufferKeyValuePartitionReader#get} after
   * {@link ByteBufferKeyValuePartitionReader#close}, expect an exception thrown.
   */
  @Test
  public void getAfterCloseTest() throws Exception {
    mReader.close();
    mThrown.expect(IllegalStateException.class);
    mReader.get(KEY1);
  }
}
