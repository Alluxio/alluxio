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

package alluxio.client.keyvalue;

import alluxio.client.ByteArrayOutStream;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Unit test of {@link BasePayloadWriter}.
 */
public class PayloadReaderWriterTest {

  private static final byte[] KEY1 = "key1".getBytes();
  private static final byte[] KEY2 = "key2_foo".getBytes();
  private static final byte[] VALUE1 = "value1".getBytes();
  private static final byte[] VALUE2 = "value2_bar".getBytes();

  private ByteArrayOutStream mTestOutStream = new ByteArrayOutStream();
  private BasePayloadWriter mTestWriter = new BasePayloadWriter(mTestOutStream);
  private BasePayloadReader mTestReader;

  /**
   * Tests {@link BasePayloadWriter#insert} by adding zero-byte key or value.
   */
  @Test
  public void addZeroLengthKeyOrValue() throws Exception {
    int offset;
    int expectedLength = 0;

    // Both key and value are empty, expect only 8 bytes of two integer length values
    offset = mTestWriter.insert("".getBytes(), "".getBytes());
    Assert.assertEquals(expectedLength, offset);

    mTestWriter.flush();
    expectedLength += 8;
    Assert.assertEquals(expectedLength, mTestOutStream.getBytesWritten());

    offset = mTestWriter.insert(KEY1, "".getBytes());
    Assert.assertEquals(expectedLength, offset);

    mTestWriter.flush();
    expectedLength += 8 + KEY1.length;
    Assert.assertEquals(expectedLength, mTestOutStream.getBytesWritten());

    offset = mTestWriter.insert("".getBytes(), VALUE1);
    Assert.assertEquals(expectedLength, offset);

    mTestWriter.flush();
    expectedLength += 8 + VALUE1.length;
    Assert.assertEquals(expectedLength, mTestOutStream.getBytesWritten());
  }

  /**
   * Tests {@link BasePayloadWriter#insert} by adding multiple key-value pairs.
   */
  @Test
  public void addMultipleKeyAndValuePairs() throws Exception {
    int offset;
    int expectedLength = 0;

    offset = mTestWriter.insert(KEY1, VALUE1);
    Assert.assertEquals(expectedLength, offset);

    mTestWriter.flush();
    expectedLength += 8 + KEY1.length + VALUE1.length;
    Assert.assertEquals(expectedLength, mTestOutStream.getBytesWritten());

    offset = mTestWriter.insert(KEY2, VALUE2);
    Assert.assertEquals(expectedLength, offset);

    mTestWriter.flush();
    expectedLength += 8 + KEY2.length + VALUE2.length;
    Assert.assertEquals(expectedLength, mTestOutStream.getBytesWritten());
  }

  /**
   * Tests {@link BasePayloadReader#getKey} to read data at offset 0.
   */
  @Test
  public void getKeyAndValueZeroOffset() throws Exception {
    int offset = mTestWriter.insert(KEY1, VALUE1);
    Assert.assertEquals(0, offset);
    mTestWriter.close();

    ByteBuffer buf = ByteBuffer.wrap(mTestOutStream.toByteArray());
    mTestReader = new BasePayloadReader(buf);
    Assert.assertEquals(ByteBuffer.wrap(KEY1), mTestReader.getKey(0));
    Assert.assertEquals(ByteBuffer.wrap(VALUE1), mTestReader.getValue(0));
  }

  /**
   * Tests {@link BasePayloadReader#getKey} to read data at non-zero offset.
   */
  @Test
  public void getKeyAndValueNonZeroOffset() throws Exception {
    mTestOutStream.write("meaningless padding".getBytes());
    int offset = mTestWriter.insert(KEY1, VALUE1);
    mTestWriter.close();

    ByteBuffer buf = ByteBuffer.wrap(mTestOutStream.toByteArray());
    mTestReader = new BasePayloadReader(buf);
    Assert.assertEquals(ByteBuffer.wrap(KEY1), mTestReader.getKey(offset));
    Assert.assertEquals(ByteBuffer.wrap(VALUE1), mTestReader.getValue(offset));
  }
}
