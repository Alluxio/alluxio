package tachyon.examples;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Config;
import tachyon.CommonUtils;

/**
 * Pure system performance test based on JAVA.
 * 
 * @author Haoyuan
 */
public class PureSystemPerformanceTest {
  private static Logger LOG = LoggerFactory.getLogger(PureSystemPerformanceTest.class);

  private static int TIMES = -1;
  private static int BLOCK_SIZE_BYTES = -1;
  private static int BLOCKS_PER_PARTITION = -1;
  private static boolean DEBUG_MODE = false;

  public static void writeParition() throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(BLOCK_SIZE_BYTES);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < BLOCK_SIZE_BYTES / 4; k ++) {
      buf.putInt(k);
    }

    LOG.info("Done write buf init: " + buf);
    if (DEBUG_MODE) {
      buf.flip();
      CommonUtils.printByteBuffer(LOG, buf);
    }
    buf.flip();

    LOG.info("Trying to write to " + Config.WORKER_DATA_FOLDER + "/RawTest");
    for (int times = 0; times < TIMES; times ++) {
      long startTimeMs = System.currentTimeMillis();
      RandomAccessFile file = 
          new RandomAccessFile(Config.WORKER_DATA_FOLDER + "/RawTest" + times, "rw");
      for (int k = 0; k < BLOCKS_PER_PARTITION; k ++) {
//        long localStartTimeNs = System.nanoTime();
        MappedByteBuffer fileByteBuffer = file.getChannel().map(
            MapMode.READ_WRITE, buf.capacity() * k, buf.capacity());
        fileByteBuffer.order(ByteOrder.nativeOrder());
        buf.array()[0] = (byte) (k);
        fileByteBuffer.put(buf.array());
//        Utils.printTimeTakenNs(localStartTimeNs, LOG, 
//            "Wrote " + (k + 1) + "th " + BLOCK_SIZE_BYTES + " bytes taken: ");
      }
      file.close();
      long takenTimeMs = System.currentTimeMillis() - startTimeMs + 1;
      CommonUtils.printTimeTakenMs(startTimeMs, LOG, "Total time taken: ");
      double result = BLOCK_SIZE_BYTES * 1000L * BLOCKS_PER_PARTITION / takenTimeMs / 1024 / 1024;
      LOG.info(times + "th Write Performance: " + result + "Mb/sec");
    }
    LOG.info("Done write buf");
  }

  public static void readPartition() throws IOException {
    ByteBuffer buf;

    LOG.info("Trying to read data...");
    if (DEBUG_MODE) {
      RandomAccessFile file =
          new RandomAccessFile(Config.WORKER_DATA_FOLDER + "/RawTest0", "rw");
      LOG.info("Verifying the reading data...");
      long startTimeMs = System.currentTimeMillis();
      buf = file.getChannel().map(MapMode.READ_ONLY, 0, file.length());
      buf.order(ByteOrder.nativeOrder());
      IntBuffer intBuf;
      intBuf = buf.asIntBuffer();
      int tmp;
      for (int i = 0; i < BLOCKS_PER_PARTITION; i ++) {
        for (int k = 0; k < BLOCK_SIZE_BYTES / 4; k ++) {
          tmp = intBuf.get();
          if ((k == 0 && tmp == i) || (k != 0 && tmp == k)) {
            System.out.print(" " + tmp);
          } else {
            CommonUtils.runtimeException("WHAT? " + tmp + " " + k);
          }
        }
        System.out.println();
      }

      file.close();
      long takenTimeMs = System.currentTimeMillis() - startTimeMs + 1;
      double result = BLOCK_SIZE_BYTES * 1000L * BLOCKS_PER_PARTITION / takenTimeMs / 1024 / 1024;
      LOG.info("Read Performance: " + result + "Mb/sec");
      LOG.info("Verifying read data: " + buf + " took " + takenTimeMs + " ms");
    }

    int[] readArray = new int[BLOCK_SIZE_BYTES / 4];

    for (int times = 0; times < TIMES; times ++) {
      RandomAccessFile file =
          new RandomAccessFile(Config.WORKER_DATA_FOLDER + "/RawTest" + times, "rw");
      long startTimeMs = System.currentTimeMillis();
      buf = file.getChannel().map(MapMode.READ_ONLY, 0, file.length());
      buf.order(ByteOrder.nativeOrder());
      IntBuffer intBuf;
      intBuf = buf.asIntBuffer();
      int tmp = 0;
      for (int i = 0; i < BLOCKS_PER_PARTITION; i ++) {
        intBuf.get(readArray);
        tmp = readArray[0];
      }
      long takenTimeMs = System.currentTimeMillis() - startTimeMs + 1;
      double result = BLOCK_SIZE_BYTES * 1000L * BLOCKS_PER_PARTITION / takenTimeMs / 1024 / 1024;
      LOG.info("Read Performance: " + result + "Mb/sec; " + tmp);

      LOG.info(times + "th Read data: " + buf + " took " + takenTimeMs + " ms");
      if (DEBUG_MODE) {
        for (int k = 0; k < readArray.length; k ++) {
          System.out.print(" " + readArray[k]);
        }
        System.out.println();
        CommonUtils.printByteBuffer(LOG, buf);
      }
      file.close();
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 4) {
      System.out.println("java -cp target/tachyon-1.0-SNAPSHOT-jar-with-dependencies.jar " +
          "tachyon.examples.PureSystemPerformanceTest " + 
          " <BlockSizeInBytes> <BlocksPerPartition> <DebugMode:true/false> <Times>\n" +
          " For example: \n" +
          "java -cp target/tachyon-1.0-SNAPSHOT-jar-with-dependencies.jar " +
          "tachyon.examples.PureSystemPerformanceTest 524288000 3 false 10");
      System.exit(-1);
    }
    BLOCK_SIZE_BYTES = Integer.parseInt(args[0]);
    BLOCKS_PER_PARTITION = Integer.parseInt(args[1]);
    DEBUG_MODE = ("true".equals(args[2]));
    TIMES = Integer.parseInt(args[3]);

    writeParition();
    readPartition();
  }
}
