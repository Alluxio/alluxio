package tachyon.worker.keyvalue;

import org.junit.Assert;
import org.junit.Test;
import tachyon.client.file.ByteArrayOutStream;

/**
 * Unit test of {@link PayloadWriter}.
 */
public class PayloadReaderWriterTest {

  private static final byte[] KEY1 = "key1".getBytes();
  private static final byte[] KEY2 = "key2_foo".getBytes();
  private static final byte[] VALUE1 = "value1".getBytes();
  private static final byte[] VALUE2 = "value2_bar".getBytes();

  private ByteArrayOutStream mTestOutStream = new ByteArrayOutStream();
  private PayloadWriter mTestWriter = new PayloadWriter(mTestOutStream);
  private PayloadReader mTestReader;

  @Test
  public void addZeroLengthKeyOrValueTest() throws Exception {
    int offset;
    int expectedLength = 0;

    // Both key and value are empty, expect only 8 bytes of two integer length values
    offset = mTestWriter.addKeyAndValue("".getBytes(), "".getBytes());
    Assert.assertEquals(expectedLength, offset);

    mTestWriter.flush();
    expectedLength += 8;
    Assert.assertEquals(expectedLength, mTestOutStream.getCount());

    offset = mTestWriter.addKeyAndValue(KEY1, "".getBytes());
    Assert.assertEquals(expectedLength, offset);

    mTestWriter.flush();
    expectedLength += 8 + KEY1.length;
    Assert.assertEquals(expectedLength, mTestOutStream.getCount());

    offset = mTestWriter.addKeyAndValue("".getBytes(), VALUE1);
    Assert.assertEquals(expectedLength, offset);

    mTestWriter.flush();
    expectedLength += 8 + VALUE1.length;
    Assert.assertEquals(expectedLength, mTestOutStream.getCount());
  }

  @Test
  public void addMultipleKeyAndValuePairsTest() throws Exception {
    int offset;
    int expectedLength = 0;

    offset = mTestWriter.addKeyAndValue(KEY1, VALUE1);
    Assert.assertEquals(expectedLength, offset);

    mTestWriter.flush();
    expectedLength += 8 + KEY1.length + VALUE1.length;
    Assert.assertEquals(expectedLength, mTestOutStream.getCount());

    offset = mTestWriter.addKeyAndValue(KEY2, VALUE2);
    Assert.assertEquals(expectedLength, offset);

    mTestWriter.flush();
    expectedLength += 8 + KEY2.length + VALUE2.length;
    Assert.assertEquals(expectedLength, mTestOutStream.getCount());
  }

  @Test
  public void getKeyAndValueZeroOffsetTest() throws Exception {
    int offset = mTestWriter.addKeyAndValue(KEY1, VALUE1);
    Assert.assertEquals(0, offset);
    mTestWriter.close();

    byte[] buf = mTestOutStream.toByteArray();
    mTestReader = new PayloadReader(buf);
    Assert.assertArrayEquals(KEY1, mTestReader.getKey(0));
    Assert.assertArrayEquals(VALUE1, mTestReader.getValue(0));
  }

  @Test
  public void getKeyAndValueNonZeroOffsetTest() throws Exception {
    mTestOutStream.write("meaningless padding".getBytes());
    int offset = mTestWriter.addKeyAndValue(KEY1, VALUE1);
    mTestWriter.close();

    byte[] buf = mTestOutStream.toByteArray();
    mTestReader = new PayloadReader(buf);
    Assert.assertArrayEquals(KEY1, mTestReader.getKey(offset));
    Assert.assertArrayEquals(VALUE1, mTestReader.getValue(offset));
  }
}
