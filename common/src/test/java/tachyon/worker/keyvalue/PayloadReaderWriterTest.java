package tachyon.worker.keyvalue;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test of {@link PayloadWriter}.
 */
public class PayloadReaderWriterTest {

  private static final byte[] KEY1 = "key1".getBytes();
  private static final byte[] KEY2 = "key2_foo".getBytes();
  private static final byte[] VALUE1 = "value1".getBytes();
  private static final byte[] VALUE2 = "value2_bar".getBytes();

  private Utils.MockOutputStream mTestOutput = new Utils.MockOutputStream(1024);
  private PayloadWriter mTestWriter = new PayloadWriter(mTestOutput);
  private PayloadReader mTestReader;

  @Test
  public void addZeroLengthKeyOrValueTest() throws Exception {
    int expectedLength = 0;

    // Both key and value are empty, expect only 8 bytes of two integer length values
    mTestWriter.addKeyAndValue("".getBytes(), "".getBytes());
    mTestWriter.flush();
    expectedLength += 8;
    Assert.assertEquals(expectedLength, mTestOutput.getCount());

    mTestWriter.addKeyAndValue(KEY1, "".getBytes());
    mTestWriter.flush();
    expectedLength += 8 + KEY1.length;
    Assert.assertEquals(expectedLength, mTestOutput.getCount());

    mTestWriter.addKeyAndValue("".getBytes(), VALUE1);
    mTestWriter.flush();
    expectedLength += 8 + VALUE1.length;
    Assert.assertEquals(expectedLength, mTestOutput.getCount());
  }

  @Test
  public void addMultipleKeyAndValuePairsTest() throws Exception {
    int expectedLength = 0;

    mTestWriter.addKeyAndValue(KEY1, VALUE1);
    mTestWriter.flush();
    expectedLength += 8 + KEY1.length + VALUE1.length;
    Assert.assertEquals(expectedLength, mTestOutput.getCount());

    mTestWriter.addKeyAndValue(KEY2, VALUE2);
    mTestWriter.flush();
    expectedLength += 8 + KEY2.length + VALUE2.length;
    Assert.assertEquals(expectedLength, mTestOutput.getCount());
  }

  @Test
  public void getKeyAndValueZeroOffsetTest() throws Exception {
    mTestWriter.addKeyAndValue(KEY1, VALUE1);
    mTestWriter.close();

    byte[] buf = mTestOutput.toByteArray();
    mTestReader = new PayloadReader(buf);
    Assert.assertArrayEquals(KEY1, mTestReader.getKey(0));
    Assert.assertArrayEquals(VALUE1, mTestReader.getValue(0));
  }

  @Test
  public void getKeyAndValueNonZeroOffsetTest() throws Exception {
    mTestWriter.addKeyAndValue(KEY1, VALUE1);
    int offset = mTestOutput.getCount();
    mTestWriter.addKeyAndValue(KEY2, VALUE2);
    mTestWriter.close();

    byte[] buf = mTestOutput.toByteArray();
    mTestReader = new PayloadReader(buf);
    Assert.assertArrayEquals(KEY2, mTestReader.getKey(offset));
    Assert.assertArrayEquals(VALUE2, mTestReader.getValue(offset));
  }
}
