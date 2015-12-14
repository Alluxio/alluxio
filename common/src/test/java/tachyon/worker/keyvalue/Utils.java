package tachyon.worker.keyvalue;

import tachyon.client.file.AbstractOutStream;

import java.io.ByteArrayOutputStream;

/**
 * Collection of util classes/functions for key-value related tests.
 */
public class Utils {
  /**
   * A mock class to capture output of {@link PayloadWriter}
   */
  public static class MockOutStream extends AbstractOutStream {
    ByteArrayOutputStream mOut;

    public MockOutStream() {
      mOut = new ByteArrayOutputStream(1000);
    }

    public void write(int b) {
      mOut.write(b);
      mCount ++;
    }

    /**
     * @return the number of bytes written to this output stream
     */
    public int getCount() {
      return mCount;
    }

    public byte[] toByteArray() {
      return mOut.toByteArray();
    }
  }
}
