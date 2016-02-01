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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.client.ByteArrayOutStream;
import tachyon.client.ClientContext;
import tachyon.client.util.ClientTestUtils;

/**
 * Unit test of {@link BaseKeyValuePartitionWriter}.
 */
public final class BaseKeyValuePartitionWriterTest {
  private static final byte[] KEY1 = "key1".getBytes();
  private static final byte[] KEY2 = "key2_foo".getBytes();
  private static final byte[] VALUE1 = "value1".getBytes();
  private static final byte[] VALUE2 = "value2_bar".getBytes();

  private ByteArrayOutStream mOutStream = new ByteArrayOutStream();
  private BaseKeyValuePartitionWriter mWriter = new BaseKeyValuePartitionWriter(mOutStream);

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  /**
   * Tests {@link BaseKeyValuePartitionWriter#put(byte[], byte[])}.
   */
  @Test
  public void putTest() throws Exception {
    mWriter.put(KEY1, VALUE1);
  }

  /**
   * Tests {@link BaseKeyValuePartitionWriter#put(byte[], byte[])}, expecting it failed when
   * writing to a closed writer.
   */
  @Test
  public void putAfterCloseTest() throws Exception {
    mWriter.close();
    mThrown.expect(IllegalStateException.class);
    mWriter.put(KEY1, VALUE1);
  }

  /**
   * Tests {@link BaseKeyValuePartitionWriter#put(byte[], byte[])}, expecting it failed when
   * writing to a canceled writer.
   */
  @Test
  public void putAfterCancelTest() throws Exception {
    mWriter.cancel();
    mThrown.expect(IllegalStateException.class);
    mWriter.put(KEY1, VALUE1);
  }

  /**
   * Tests {@link BaseKeyValuePartitionWriter#close()} after
   * {@link BaseKeyValuePartitionWriter#cancel()}, expecting a close is a no-op after the
   * previous cancel.
   */
  @Test
  public void closeAfterCancelTest() throws Exception {
    mWriter.cancel();
    Assert.assertTrue(mOutStream.isClosed());
    Assert.assertTrue(mOutStream.isCanceled());

    // Expect close to be a no-op
    mWriter.close();
    Assert.assertTrue(mOutStream.isClosed());
    Assert.assertTrue(mOutStream.isCanceled());
  }

  /**
   * Tests {@link BaseKeyValuePartitionWriter#close()} after
   * {@link BaseKeyValuePartitionWriter#close()}, expecting a close is a no-op after the
   * previous close.
   */
  @Test
  public void closeAfterCloseTest() throws Exception {
    // Expect the underline stream to be closed
    mWriter.close();
    Assert.assertTrue(mOutStream.isClosed());
    Assert.assertFalse(mOutStream.isCanceled());

    // Expect close to be a no-op
    mWriter.close();
    Assert.assertTrue(mOutStream.isClosed());
    Assert.assertFalse(mOutStream.isCanceled());
  }

  /**
   * Tests {@link BaseKeyValuePartitionWriter#put} and then {@link BaseKeyValuePartitionReader#get}.
   */
  @Test
  public void putAndGetTest() throws Exception {
    mWriter.put(KEY1, VALUE1);
    mWriter.put(KEY2, VALUE2);
    mWriter.close();
    byte[] fileData = mOutStream.toByteArray();
    ByteBufferKeyValuePartitionReader reader =
        new ByteBufferKeyValuePartitionReader(ByteBuffer.wrap(fileData));
    Assert.assertArrayEquals(VALUE1, reader.get(KEY1));
    Assert.assertArrayEquals(VALUE2, reader.get(KEY2));

    Assert.assertNull(reader.get("NoSuchKey".getBytes()));
  }

  /**
   * Tests {@link BaseKeyValuePartitionWriter#canPut} works.
   */
  @Test
  public void canPutKeyValueTest() throws Exception {
    long size = mWriter.byteCount() + KEY1.length + VALUE1.length + 2 * Constants.BYTES_IN_INTEGER;
    ClientContext.getConf().set(Constants.KEY_VALUE_PARTITION_SIZE_BYTES_MAX, String.valueOf(size));
    mWriter = new BaseKeyValuePartitionWriter(mOutStream);
    Assert.assertTrue(mWriter.canPut(KEY1, VALUE1));
    mWriter.put(KEY1, VALUE1);
    Assert.assertFalse(mWriter.canPut(KEY1, VALUE1));
    ClientTestUtils.resetClientContext();
  }

  /**
   * Tests {@link BaseKeyValuePartitionWriter#keyCount()} works.
   */
  @Test
  public void keyCountTest() throws Exception {
    Assert.assertEquals(0, mWriter.keyCount());
    mWriter.put(KEY1, VALUE1);
    Assert.assertEquals(1, mWriter.keyCount());
    mWriter.put(KEY2, VALUE2);
    Assert.assertEquals(2, mWriter.keyCount());
  }

  /**
   * Tests {@link BaseKeyValuePartitionWriter#byteCount()} works.
   */
  @Test
  public void byteCountTest() throws Exception {
    mWriter.put(KEY1, VALUE1);
    Assert.assertTrue(mWriter.byteCount() > 0);
  }
}
