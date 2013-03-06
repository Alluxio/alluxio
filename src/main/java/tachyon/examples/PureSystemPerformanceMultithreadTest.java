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

import com.sun.corba.se.impl.util.Version;

import tachyon.Config;
import tachyon.CommonUtils;

public class PureSystemPerformanceMultithreadTest {
  private static Logger LOG = LoggerFactory.getLogger(PureSystemPerformanceMultithreadTest.class);

  private static int TIMES = -1;
  private static int BLOCK_SIZE_BYTES = -1;
  private static int BLOCKS_PER_PARTITION = -1;
  private static boolean DEBUG_MODE = false;
  private static int THREADS = -1;

  public static class MemCopyWorker extends Thread {
    private int mWorkerId;
    private int mLeft;
    private int mRight;
    private ByteBuffer mBuf;

    public MemCopyWorker(int id, int left, int right, ByteBuffer buf) {
      mWorkerId = id;
      mLeft = left;
      mRight = right;
      mBuf = buf;
    }

    public void memCopyParition() throws IOException {
      if (DEBUG_MODE) {
        mBuf.flip();
        CommonUtils.printByteBuffer(LOG, mBuf);
      }
      mBuf.flip();

      ByteBuffer dst = null; 
      for (int times = mLeft; times < mRight; times ++) {
        long startTimeMs = System.currentTimeMillis();

        if (times == mLeft) {
          dst = ByteBuffer.allocateDirect(BLOCK_SIZE_BYTES * BLOCKS_PER_PARTITION);
          dst.order(ByteOrder.nativeOrder());
        }
        for (int k = 0; k < BLOCKS_PER_PARTITION; k ++) {
          mBuf.array()[0] = (byte) (k + mWorkerId);
          dst.put(mBuf.array());
        }
        long takenTimeMs = System.currentTimeMillis() - startTimeMs;
        CommonUtils.printTimeTakenMs(startTimeMs, LOG, "Total time taken: ");
        double result = BLOCK_SIZE_BYTES * 1000L * BLOCKS_PER_PARTITION / takenTimeMs / 1024 / 1024;
        LOG.info(times + "th MemCopy @ Worker " + mWorkerId + "'s Performance: " + result + "Mb/sec");
        dst.clear();
      }
    }

    @Override
    public void run() {
      try {
        memCopyParition();
      } catch (IOException e) {
        CommonUtils.runtimeException(e);
      }
      LOG.info("MemCopyWorker " + mWorkerId + " just finished.");
    }
  }

  public static class WriterWorker extends Thread {
    private int mWorkerId;
    private int mLeft;
    private int mRight;
    private ByteBuffer mBuf;

    public WriterWorker(int id, int left, int right, ByteBuffer buf) {
      mWorkerId = id;
      mLeft = left;
      mRight = right;
      mBuf = buf;
    }

    public void writeParition() throws IOException {
      if (DEBUG_MODE) {
        mBuf.flip();
        CommonUtils.printByteBuffer(LOG, mBuf);
      }
      mBuf.flip();

      for (int times = mLeft; times < mRight; times ++) {
        LOG.info("Trying to write to " + Config.WORKER_DATA_FOLDER + "/RawTest" + times);
        long startTimeMs = System.currentTimeMillis();

        RandomAccessFile file = 
            new RandomAccessFile(Config.WORKER_DATA_FOLDER + "/RawTest" + times, "rw");
        for (int k = 0; k < BLOCKS_PER_PARTITION; k ++) {
          MappedByteBuffer fileByteBuffer = file.getChannel().map(
              MapMode.READ_WRITE, mBuf.capacity() * k, mBuf.capacity());
          fileByteBuffer.order(ByteOrder.nativeOrder());
          mBuf.array()[0] = (byte) (k + mWorkerId);
          fileByteBuffer.put(mBuf.array());
        }
        file.close();
        long takenTimeMs = System.currentTimeMillis() - startTimeMs + 1;
        CommonUtils.printTimeTakenMs(startTimeMs, LOG, "Total time taken: ");
        double result = BLOCK_SIZE_BYTES * 1000L * BLOCKS_PER_PARTITION / takenTimeMs / 1024 / 1024;
        LOG.info(times + "th Write @ Worker " + mWorkerId + "'s Performance: " + result + "Mb/sec");
      }
    }

    @Override
    public void run() {
      try {
        writeParition();
      } catch (IOException e) {
        CommonUtils.runtimeException(e);
      }
      LOG.info("WriteWorker " + mWorkerId + " just finished.");
    }
  }

  public static class ReadWorker extends Thread {
    private int mWorkerId;
    private int mLeft;
    private int mRight;

    public ReadWorker(int id, int left, int right) {
      mWorkerId = id;
      mLeft = left;
      mRight = right;
    }

    public void readPartition() throws IOException {
      ByteBuffer buf;

      LOG.info("Trying to read data...");
      if (DEBUG_MODE) {
        RandomAccessFile file =
            new RandomAccessFile(Config.WORKER_DATA_FOLDER + "/RawTest" + mLeft, "rw");
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
            if ((k == 0 && tmp == (i + mWorkerId)) || (k != 0 && tmp == k)) {
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
        LOG.info("Read Performance: " + result + "Mb/sec @ Worker " + mWorkerId);
        LOG.info("Verifying read data: " + buf + " took " + takenTimeMs + " ms @ Worker " + mWorkerId);
      }

      int[] readArray = new int[BLOCK_SIZE_BYTES / 4];

      for (int times = mLeft; times < mRight; times ++) {
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

        LOG.info(times + "th Read @ Worker " + mWorkerId + "'s Performance: " + result + 
            "Mb/sec, took " + takenTimeMs + " ms. " + tmp);
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

    @Override
    public void run() {
      try {
        readPartition();
      } catch (IOException e) {
        CommonUtils.runtimeException(e);
      }
      LOG.info("ReadWorker " + mWorkerId + " just finished.");
    }
  }

  private static void memoryCopyTest() {
    ByteBuffer[] bufs = new ByteBuffer[THREADS];

    for (int thread = 0; thread < THREADS; thread ++) {
      ByteBuffer sRawData = ByteBuffer.allocateDirect(BLOCK_SIZE_BYTES);
      sRawData.order(ByteOrder.nativeOrder());
      for (int k = 0; k < BLOCK_SIZE_BYTES / 4; k ++) {
        sRawData.putInt(k);
      }
      bufs[thread] = sRawData;
    }

    MemCopyWorker[] WWs = new MemCopyWorker[THREADS];
    int t = TIMES / THREADS;
    for (int thread = 0; thread < THREADS; thread ++) {
      WWs[thread] = new MemCopyWorker(thread, t * thread, t * (thread + 1), bufs[thread]);
    }

    long startTimeMs = System.currentTimeMillis();
    for (int thread = 0; thread < THREADS; thread ++) {
      WWs[thread].start();
    }
    for (int thread = 0; thread < THREADS; thread ++) {
      try {
        WWs[thread].join();
      } catch (InterruptedException e) {
        CommonUtils.runtimeException(e);
      }
    }
    long takenTimeMs = System.currentTimeMillis() - startTimeMs;
    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "Total time taken: ");
    double result = TIMES * BLOCK_SIZE_BYTES * 1000L * BLOCKS_PER_PARTITION / takenTimeMs / 1024 / 1024;
    LOG.info("Overall Write Performance: " + result + "Mb/sec");
  }

  private static void writeTest() {
    ByteBuffer[] bufs = new ByteBuffer[THREADS];

    for (int thread = 0; thread < THREADS; thread ++) {
      ByteBuffer sRawData = ByteBuffer.allocateDirect(BLOCK_SIZE_BYTES);
      sRawData.order(ByteOrder.nativeOrder());
      for (int k = 0; k < BLOCK_SIZE_BYTES / 4; k ++) {
        sRawData.putInt(k);
      }
      bufs[thread] = sRawData;
    }

    WriterWorker[] WWs = new WriterWorker[THREADS];
    int t = TIMES / THREADS;
    for (int thread = 0; thread < THREADS; thread ++) {
      WWs[thread] = new WriterWorker(thread, t * thread, t * (thread + 1), bufs[thread]);
    }

    long startTimeMs = System.currentTimeMillis();
    for (int thread = 0; thread < THREADS; thread ++) {
      WWs[thread].start();
    }
    for (int thread = 0; thread < THREADS; thread ++) {
      try {
        WWs[thread].join();
      } catch (InterruptedException e) {
        CommonUtils.runtimeException(e);
      }
    }
    long takenTimeMs = System.currentTimeMillis() - startTimeMs;
    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "Total time taken: ");
    double result = TIMES * BLOCK_SIZE_BYTES * 1000L * BLOCKS_PER_PARTITION / takenTimeMs / 1024 / 1024;
    LOG.info("Overall Write Performance: " + result + "Mb/sec");
  }

  private static void readTest() {
    ReadWorker[] WWs = new ReadWorker[THREADS];
    int t = TIMES / THREADS;
    for (int thread = 0; thread < THREADS; thread ++) {
      WWs[thread] = new ReadWorker(thread, t * thread, t * (thread + 1));
    }

    long startTimeMs = System.currentTimeMillis();
    for (int thread = 0; thread < THREADS; thread ++) {
      WWs[thread].start();
    }
    for (int thread = 0; thread < THREADS; thread ++) {
      try {
        WWs[thread].join();
      } catch (InterruptedException e) {
        CommonUtils.runtimeException(e);
      }
    }
    long takenTimeMs = System.currentTimeMillis() - startTimeMs;
    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "Total time taken: ");
    double result = TIMES * BLOCK_SIZE_BYTES * 1000L * BLOCKS_PER_PARTITION / takenTimeMs / 1024 / 1024;
    LOG.info("Overall Read Performance: " + result + "Mb/sec");
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 5) {
      String EXEC = "java -cp target/tachyon-" + Version.VERSION + "-jar-with-dependencies.jar " +
          "tachyon.examples.PureSystemPerformanceTest ";
      System.out.println(EXEC  + 
          " <BlockSizeInBytes> <BlocksPerPartition> <DebugMode:true/false> <Times> <Threads>\n" +
          " For example: \n" +
          "a). " + EXEC + " 524288000 4 false 10 2 \n" +
          "b). " + EXEC + " 16777216 64 false 3 3 \n" +
          "c). java -Xmn2G -Xms4G -Xmx4G " +
          "-cp target/tachyon-" + Version.VERSION  + "-jar-with-dependencies.jar " +
          "tachyon.examples.PureSystemPerformanceMultithreadTest 16777216 64 false 24 12 \n");
      System.exit(-1);
    }
    BLOCK_SIZE_BYTES = Integer.parseInt(args[0]);
    BLOCKS_PER_PARTITION = Integer.parseInt(args[1]);
    DEBUG_MODE = ("true".equals(args[2]));
    TIMES = Integer.parseInt(args[3]);
    THREADS = Integer.parseInt(args[4]);
    if (TIMES % THREADS != 0) {
      CommonUtils.runtimeException("Times has to be multiple of Threads");
    }

    memoryCopyTest();
    writeTest();
    readTest();
  }
}
