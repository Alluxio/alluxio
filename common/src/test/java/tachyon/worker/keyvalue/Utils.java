package tachyon.worker.keyvalue;

import java.io.ByteArrayOutputStream;

/**
 * Collection of util classes/functions for key-value related tests.
 */
public class Utils {
  /**
   * A mock class to capture output of {@link PayloadWriter}
   */
  public static class MockOutputStream extends ByteArrayOutputStream {
    public MockOutputStream(int size) {
      super(size);
    }

    /**
     * @return the number of bytes written to this output stream
     */
    public int getCount() {
      return count;
    }
  }
}
