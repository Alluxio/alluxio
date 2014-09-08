package tachyon.client;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;

import tachyon.TachyonURI;
import tachyon.master.LocalTachyonCluster;

/**
 * These set of tests are to verify that updates to files are rejected.
 */
public final class TachyonFileUpdateTest {

  private static final int WORKER_CAPACITY_BYTES = 1000;

  private static final int USER_QUOTA_UNIT_BYTES = 100;
  private static final String USER_QUOTA_UNIT_BYTES_STR = Integer.toString(USER_QUOTA_UNIT_BYTES);

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", USER_QUOTA_UNIT_BYTES_STR);
    mLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Test
  public void rerunOutStream() throws IOException {
    run(new Write() {
      @Override
      public void apply(TachyonFile file, WriteType type, int numRuns) throws IOException {
        int length = (numRuns + 1) * 10;
        DataOutputStream os = new DataOutputStream(file.getOutStream(type));
        try {
          for (int j = 0; j < length; j ++) {
            os.writeInt(j);
          }
        } finally {
          os.close();
        }
      }
    });
  }

  @Test
  public void rerunBuffer() throws IOException {
    run(new Write() {
      @Override
      public void apply(TachyonFile file, WriteType type, int numRuns) throws IOException {
        int length = (numRuns + 1) * 10;
        ByteBuffer buffer = ByteBuffer.allocate(length * 4);
        buffer.order(ByteOrder.nativeOrder());
        for (int j = 0; j < length; j ++) {
          buffer.putInt(j);
        }
        buffer.flip();

        OutStream os = file.getOutStream(type);
        try {
          os.write(buffer.array());
        } finally {
          os.close();
        }
      }
    });
  }

  /**
   * Runs the tests against the writer. The test verifies that a IOException is returned
   * when {@link Write#apply(TachyonFile, WriteType, int)} is called more than once.
   */
  private void run(Write writer) throws IOException {
    // runs against all write types
    // any way in JUnit that lets me do this that isn't way uglier?
    // I know I can have a function return the values, then have this in my constructor
    // can I do this at test site?
    for (WriteType type : WriteType.values()) {
      String filePath = testPath(type);
      TachyonFile file = createFile(filePath);

      // first time writting should pass
      writer.apply(file, type, 0);

      // should fail the second time
      try {
        writer.apply(file, type, 1);
        Assert.fail("TachyonFile only supports write once; test against WriteType " + type);
      } catch (IOException e) {
        // good!
        Assert.assertEquals("Overriding after completion not supported.", e.getMessage());
      }

      // in case of caching, try to create the file object again
      file = mTfs.getFile(filePath, false);

      // should fail the third time as well
      try {
        writer.apply(file, type, 2);
        Assert.fail("TachyonFile only supports write once; test against WriteType " + type);
      } catch (IOException e) {
        // good!
        Assert.assertEquals("Overriding after completion not supported.", e.getMessage());
      }
    }
  }

  private TachyonFile createFile(final String path) throws IOException {
    int fileId = mTfs.createFile(path);
    TachyonFile file = mTfs.getFile(fileId);
    return file;
  }

  /**
   * Main body of the test. IOExceptions should not be caught inside the code that way the
   * {@link #run(tachyon.client.TachyonFileUpdateTest.Write)} method can verify them.
   */
  private interface Write {
    void apply(TachyonFile file, WriteType type, int numRuns) throws IOException;
  }

  /**
   * Creates a new path for the given test. The path will look like the following:
   * {@code /class-name/method-name/nano-time/write-type}
   */
  private static String testPath(final Object postfix) {
    String dir = TachyonFileUpdateTest.class.getSimpleName();
    String name = new Throwable().getStackTrace()[2].getMethodName();

    return join(dir, name, System.nanoTime(), postfix);
  }

  private static final Joiner PATH_JOINER = Joiner.on(TachyonURI.SEPARATOR);

  private static String join(Object... paths) {
    return TachyonURI.SEPARATOR + PATH_JOINER.join(paths);
  }
}
