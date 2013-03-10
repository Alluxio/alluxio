package tachyon.examples;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Config;
import tachyon.CommonUtils;
import tachyon.Version;
import tachyon.client.OpType;
import tachyon.client.TachyonClient;
import tachyon.client.TachyonFile;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.OutOfMemoryForPinFileException;
import tachyon.thrift.SuspectedFileSizeException;

public class PerformanceTest {
  private static Logger LOG = LoggerFactory.getLogger(PerformanceTest.class);

  private static TachyonClient TC;
  private static String FILE_NAME = null;
  private static int BLOCK_SIZE_BYTES = -1;
  private static int BLOCKS_PER_FILE = -1;
  private static int FILES = -1;
  private static boolean DEBUG_MODE = false;

  public static void createFiles() throws InvalidPathException {
    long startTimeMs = CommonUtils.getCurrentMs();
    for (int k = 0; k < FILES; k ++) {
      int fileId = TC.createFile(FILE_NAME + k);
      CommonUtils.printTimeTakenMs(startTimeMs, LOG, "user_createFile: with fileId " + fileId);
    }
  }

  public static void writeFiles() throws IOException, SuspectedFileSizeException, 
  OutOfMemoryForPinFileException, InvalidPathException, TException {
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
    for (int pId = 0; pId < FILES; pId ++) {
      TachyonFile file = TC.getFile(FILE_NAME + pId);
      file.open(OpType.WRITE_CACHE);
      long startTimeMs = System.currentTimeMillis();
      for (int k = 0; k < BLOCKS_PER_FILE; k ++) {
        //        long localStartTimeNs = System.nanoTime();
        buf.array()[0] = (byte) (k);
        file.append(buf);
        //        Utils.printTimeTakenNs(localStartTimeNs, LOG, String.format(
        //            "Wrote %dth %d bytes taken for Partition %d.", k + 1, BLOCK_SIZE_BYTES, pId));
      }
      file.close();
      long takenTimeMs = System.currentTimeMillis() - startTimeMs + 1;
      CommonUtils.printTimeTakenMs(startTimeMs, LOG, "Total time taken: ");
      double result = BLOCK_SIZE_BYTES * 1000L * BLOCKS_PER_FILE / takenTimeMs / 1024 / 1024;
      LOG.info("Write Performance: " + result + "Mb/sec for file " + FILE_NAME + pId);
    }
  }

  public static void readFiles() 
      throws IOException, SuspectedFileSizeException, InvalidPathException, TException {
    ByteBuffer buf;
    LOG.info("Trying to read data...");
    if (DEBUG_MODE) {
      LOG.info("Verifying the reading data...");

      for (int pId = 0; pId < FILES; pId ++) {
        TachyonFile file = TC.getFile(FILE_NAME + pId);
        file.open(OpType.READ_CACHE);

        long startTimeMs = System.currentTimeMillis();
        buf = file.readByteBuffer();
        IntBuffer intBuf;
        intBuf = buf.asIntBuffer();
        int tmp;
        for (int i = 0; i < BLOCKS_PER_FILE; i ++) {
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

        long takenTimeMs = System.currentTimeMillis() - startTimeMs + 1;
        double result = BLOCK_SIZE_BYTES * 1000L * BLOCKS_PER_FILE / takenTimeMs / 1024 / 1024;
        LOG.info("Read Performance: " + result + "Mb/sec");
        LOG.info("Verifying read data: " + buf + " took " + takenTimeMs + " ms");
      }
    }

    for (int pId = 0; pId < FILES; pId ++) {
      TachyonFile file = TC.getFile(FILE_NAME + pId);
      file.open(OpType.READ_CACHE);

      int[] readArray = new int[BLOCK_SIZE_BYTES / 4];
      long startTimeMs = System.currentTimeMillis();
      buf = file.readByteBuffer();
      IntBuffer intBuf;
      intBuf = buf.asIntBuffer();
      int tmp = 0;
      for (int i = 0; i < BLOCKS_PER_FILE; i ++) {
        intBuf.get(readArray);
        tmp = readArray[0];
      }
      file.close();
      long takenTimeMs = System.currentTimeMillis() - startTimeMs + 1;
      double result = BLOCK_SIZE_BYTES * 1000L * BLOCKS_PER_FILE / takenTimeMs / 1024 / 1024;
      LOG.info("Read Performance: " + result + "Mb/sec. " + tmp);
      LOG.info("Read data: " + buf);
      if (DEBUG_MODE) {
        buf.flip();
        CommonUtils.printByteBuffer(LOG, buf);
      }
    }
  }

  public static void main(String[] args) throws IOException, SuspectedFileSizeException,
  OutOfMemoryForPinFileException, InvalidPathException, TException {
    if (args.length != 6) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION + 
          "-jar-with-dependencies.jar " +
          "tachyon.examples.PerformanceTest " + " <MasterIp> <FileName> " +
          "<BlockSizeInBytes> <BlocksPerFile> <DebugMode:true/false> <NumberOfFiles>");
      System.exit(-1);
    }
    TC = TachyonClient.getClient(new InetSocketAddress(args[0], Config.MASTER_PORT));
    FILE_NAME = args[1];
    BLOCK_SIZE_BYTES = Integer.parseInt(args[2]);
    BLOCKS_PER_FILE = Integer.parseInt(args[3]);
    DEBUG_MODE = ("true".equals(args[4]));
    FILES = Integer.parseInt(args[5]);

    createFiles();
    writeFiles();
    readFiles();
  }
}