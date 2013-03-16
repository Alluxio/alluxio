package tachyon.examples;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel.MapMode;

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
import tachyon.thrift.SuspectedFileSizeException;

public class PerformanceTest {
  private static Logger LOG = LoggerFactory.getLogger(PerformanceTest.class);

  private static final int RESULT_ARRAY_SIZE = 68;
  private static final String FOLDER = "/mnt/ramdisk/";

  private static TachyonClient MTC = null;
  private static String MASTER_HOST = null;
  private static String FILE_NAME = null;
  private static int WRITE_BLOCK_SIZE_BYTES = -1;
  private static int BLOCKS_PER_FILE = -1;
  private static int THREADS = -1;
  private static int FILES = -1;
  private static boolean DEBUG_MODE = false;
  private static int FILE_BYTES = -1;
  private static long FILES_BYTES = -1;
  private static String RESULT_PREFIX = null;
  private static long[] Results = new long[RESULT_ARRAY_SIZE];
  private static int BASE_FILE_NUMBER = 0;

  public static void createFiles() throws InvalidPathException {
    long startTimeMs = CommonUtils.getCurrentMs();
    for (int k = 0; k < THREADS; k ++) {
      int fileId = MTC.createFile(FILE_NAME + k);
      CommonUtils.printTimeTakenMs(startTimeMs, LOG, "user_createFiles with fileId " + fileId);
    }
  }

  public static void logPerIteration(long startTimeMs, int times, String msg, int workerId) {
    long takenTimeMs = System.currentTimeMillis() - startTimeMs;
    double result = 1000L * FILE_BYTES / takenTimeMs / 1024 / 1024;
    LOG.info(times + msg + workerId + " : " + result + " Mb/sec. Took " + 
        takenTimeMs + " ms. ");
  }

  public static abstract class Worker extends Thread {
    protected int mWorkerId;
    protected int mLeft;
    protected int mRight;
    protected ByteBuffer mBuf;

    public Worker(int id, int left, int right, ByteBuffer buf) {
      mWorkerId = id;
      mLeft = left;
      mRight = right;
      mBuf = buf;
    }
  }

  public static class GeneralWorker extends Worker {
    private boolean mOneToMany;
    private boolean mMemoryOnly;
    private String mMsg;

    public GeneralWorker(int id, int left, int right, ByteBuffer buf, boolean oneToMany,
        boolean memoryOnly, String msg) {
      super(id, left, right, buf);
      mOneToMany = oneToMany;
      mMemoryOnly = memoryOnly;
      mMsg = msg;
    }

    public void memoryCopyParition() throws IOException {
      if (DEBUG_MODE) {
        mBuf.flip();
        CommonUtils.printByteBuffer(LOG, mBuf);
      }
      mBuf.flip();
      long sum = 0;
      String str = "th " + mMsg + " @ Worker ";

      if (mOneToMany) {
        ByteBuffer dst = null;
        RandomAccessFile file = null;
        if (mMemoryOnly) {
          dst = ByteBuffer.allocateDirect(FILE_BYTES);
        }
        for (int times = mLeft; times < mRight; times ++) {
          if (!mMemoryOnly) {
            file = new RandomAccessFile(FOLDER + (mWorkerId + 2 + BASE_FILE_NUMBER), "rw");
            dst = file.getChannel().map(MapMode.READ_WRITE, 0, FILE_BYTES);
          }
          long startTimeMs = System.currentTimeMillis();
          dst.order(ByteOrder.nativeOrder());
          for (int k = 0; k < BLOCKS_PER_FILE; k ++) {
            mBuf.array()[0] = (byte) (k + mWorkerId);
            dst.put(mBuf.array());
          }
          dst.clear();
          sum += dst.get(times);
          dst.clear();
          if (!mMemoryOnly) {
            file.close();
          }
          logPerIteration(startTimeMs, times, str, mWorkerId);
        }
      } else {
        ByteBuffer dst = null;
        RandomAccessFile file = null;
        if (mMemoryOnly) {
          dst = ByteBuffer.allocateDirect(FILE_BYTES);
        }
        for (int times = mLeft; times < mRight; times ++) {
          if (!mMemoryOnly) {
            file = new RandomAccessFile(FOLDER + (mWorkerId + 2 + BASE_FILE_NUMBER), "rw");
            dst = file.getChannel().map(MapMode.READ_WRITE, 0, FILE_BYTES);
          }
          dst.order(ByteOrder.nativeOrder());
          long startTimeMs = System.currentTimeMillis();
          for (int k = 0; k < BLOCKS_PER_FILE; k ++) {
            dst.get(mBuf.array());
          }
          sum += mBuf.get(times % 16);
          dst.clear();
          logPerIteration(startTimeMs, times, str, mWorkerId);
          if (!mMemoryOnly) {
            file.close();
          }
        }
      }
      Results[mWorkerId] = sum;
    }

    @Override
    public void run() {
      try {
        memoryCopyParition();
      } catch (IOException e) {
        CommonUtils.runtimeException(e);
      }
      LOG.info(mMsg + mWorkerId + " just finished.");
    }
  }

  public static class TachyonWriterWorker extends Worker {
    private TachyonClient mTC;

    public TachyonWriterWorker(int id, int left, int right, ByteBuffer buf) {
      super(id, left, right, buf);
      mTC = TachyonClient.getClient(new InetSocketAddress(MASTER_HOST, Config.MASTER_PORT));
    }

    public void writeParition()
        throws IOException, SuspectedFileSizeException, InvalidPathException, TException {
      if (DEBUG_MODE) {
        mBuf.flip();
        CommonUtils.printByteBuffer(LOG, mBuf);
      }

      mBuf.flip();
      TachyonFile file = mTC.getFile(FILE_NAME + mWorkerId);
      file.open(OpType.WRITE_CACHE);
      for (int pId = mLeft; pId < mRight; pId ++) {
        long startTimeMs = System.currentTimeMillis();
        for (int k = 0; k < BLOCKS_PER_FILE; k ++) {
          mBuf.array()[0] = (byte) (k + mWorkerId);
          file.append(mBuf);
        }
        logPerIteration(startTimeMs, pId, "th WriteTachyonFile @ Worker ", pId);
      }
      file.close();
    }

    @Override
    public void run() {
      try {
        writeParition();
      } catch (Exception e) {
        CommonUtils.runtimeException(e);
      }
      LOG.info("WriteWorker " + mWorkerId + " just finished.");
    }
  }

  public static class TachyonReadWorker extends Worker {
    private TachyonClient mTC;

    public TachyonReadWorker(int id, int left, int right, ByteBuffer buf) {
      super(id, left, right, buf);
      mTC = TachyonClient.getClient(new InetSocketAddress(MASTER_HOST, Config.MASTER_PORT));
    }

    public void readPartition() 
        throws IOException, SuspectedFileSizeException, InvalidPathException, TException {
      ByteBuffer buf;
      if (DEBUG_MODE) {
        LOG.info("Verifying the reading data...");

        for (int pId = mLeft; pId < mRight; pId ++) {
          TachyonFile file = mTC.getFile(FILE_NAME + mWorkerId);
          file.open(OpType.READ_TRY_CACHE);
          buf = file.readByteBuffer();
          IntBuffer intBuf;
          intBuf = buf.asIntBuffer();
          int tmp;
          for (int i = 0; i < BLOCKS_PER_FILE; i ++) {
            for (int k = 0; k < WRITE_BLOCK_SIZE_BYTES / 4; k ++) {
              tmp = intBuf.get();
              if ((k == 0 && tmp == (i + mWorkerId)) || (k != 0 && tmp == k)) {
              } else {
                CommonUtils.runtimeException("WHAT? " + tmp + " " + k);
              }
            }
          }
        }
      }
      TachyonFile file = mTC.getFile(FILE_NAME + mWorkerId);
      file.open(OpType.READ_TRY_CACHE);
      buf = file.readByteBuffer();
      long sum = 0;
      for (int pId = mLeft; pId < mRight; pId ++) {
        long startTimeMs = System.currentTimeMillis();
        for (int i = 0; i < BLOCKS_PER_FILE; i ++) {
          buf.get(mBuf.array());
        }
        sum += mBuf.get(pId % 16);
        logPerIteration(startTimeMs, pId, "th ReadTachyonFile @ Worker ", pId);

        if (DEBUG_MODE) {
          buf.flip();
          CommonUtils.printByteBuffer(LOG, buf);
        }
        buf.clear();
      }
      file.close();
      Results[mWorkerId] = sum;
    }

    @Override
    public void run() {
      try {
        readPartition();
      } catch (Exception e) {
        CommonUtils.runtimeException(e);
      }
      LOG.info("ReadWorker " + mWorkerId + " just finished.");
    }
  }

  private static void memoryCopyTest(boolean write, boolean memoryOnly) {
    ByteBuffer[] bufs = new ByteBuffer[THREADS];

    for (int thread = 0; thread < THREADS; thread ++) {
      ByteBuffer sRawData = ByteBuffer.allocate(WRITE_BLOCK_SIZE_BYTES);
      sRawData.order(ByteOrder.nativeOrder());
      for (int k = 0; k < WRITE_BLOCK_SIZE_BYTES / 4; k ++) {
        sRawData.putInt(k);
      }
      bufs[thread] = sRawData;
    }

    String msg = (write ? "Write" : "Read") + (memoryOnly ? "_Memory " : "_RamFile ");

    GeneralWorker[] WWs = new GeneralWorker[THREADS];
    int t = FILES / THREADS;
    for (int thread = 0; thread < THREADS; thread ++) {
      WWs[thread] = new GeneralWorker(
          thread, t * thread, t * (thread + 1), bufs[thread], write, memoryOnly, msg);
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
    double result = 1000L * FILES_BYTES / takenTimeMs / 1024 / 1024;

    LOG.info(RESULT_PREFIX + "Entire " + msg + " Test : " + result + " Mb/sec. Took " +
        takenTimeMs + " ms. Current System Time: " + System.currentTimeMillis());
  }

  private static void TachyonTest(boolean write) {
    ByteBuffer[] bufs = new ByteBuffer[THREADS];

    for (int thread = 0; thread < THREADS; thread ++) {
      ByteBuffer sRawData = ByteBuffer.allocate(WRITE_BLOCK_SIZE_BYTES);
      sRawData.order(ByteOrder.nativeOrder());
      for (int k = 0; k < WRITE_BLOCK_SIZE_BYTES / 4; k ++) {
        sRawData.putInt(k);
      }
      bufs[thread] = sRawData;
    }

    Worker[] WWs = new Worker[THREADS];
    int t = FILES / THREADS;
    for (int thread = 0; thread < THREADS; thread ++) {
      if (write) {
        WWs[thread] = new TachyonWriterWorker(thread, t * thread, t * (thread + 1), bufs[thread]);
      } else {
        WWs[thread] = new TachyonReadWorker(thread, t * thread, t * (thread + 1), bufs[thread]);
      }
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
    double result = FILES_BYTES * 1000L / takenTimeMs / 1024 / 1024;
    LOG.info(RESULT_PREFIX + "Entire " + (write ? "Write ": "Read ") + result + " Mb/sec. Took " +
        takenTimeMs + " ms. Current System Time: " + System.currentTimeMillis());
  }

  public static void main(String[] args) throws IOException, InvalidPathException {
    if (args.length != 9) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION + 
          "-jar-with-dependencies.jar tachyon.examples.PerformanceTest " + 
          "<MasterIp> <FileName> <WriteBlockSizeInBytes> <BlocksPerFile> " +
          "<DebugMode:true/false> <Threads> <FilesPerThread> <TestCaseNumber> <BaseFileNumber>\n" +
          "1: Files Write Test\n" +
          "2: Files Read Test\n" + 
          "3: RamFile Write Test \n" +
          "4: RamFile Read Test \n" + 
          "5: ByteBuffer Write Test \n" +
          "6: ByteBuffer Read Test \n");
      System.exit(-1);
    }

    MASTER_HOST = args[0];
    FILE_NAME = args[1];
    WRITE_BLOCK_SIZE_BYTES = Integer.parseInt(args[2]);
    BLOCKS_PER_FILE = Integer.parseInt(args[3]);
    DEBUG_MODE = ("true".equals(args[4]));
    THREADS = Integer.parseInt(args[5]);
    FILES = Integer.parseInt(args[6]) * THREADS;
    int testCaseNumber = Integer.parseInt(args[7]);
    BASE_FILE_NUMBER = Integer.parseInt(args[8]);

    FILE_BYTES = BLOCKS_PER_FILE * WRITE_BLOCK_SIZE_BYTES;
    FILES_BYTES = 1L * FILE_BYTES * FILES;

    RESULT_PREFIX = String.format("Threads %d FilesPerThread %d TotalFiles %d " +
        "BLOCK_SIZE_KB %d BLOCKS_PER_FILE %d FILE_SIZE_MB %d " +
        "Tachyon_WRITE_BUFFER_SIZE_KB %d : ",
        THREADS, FILES / THREADS, FILES, WRITE_BLOCK_SIZE_BYTES / 1024, 
        BLOCKS_PER_FILE, CommonUtils.getMB(FILE_BYTES), 
        Config.USER_BUFFER_PER_PARTITION_BYTES / 1024);

    if (testCaseNumber == 1) {
      RESULT_PREFIX = "TachyonFilesWriteTest " + RESULT_PREFIX;
      LOG.info(RESULT_PREFIX);
      MTC = TachyonClient.getClient(new InetSocketAddress(MASTER_HOST, Config.MASTER_PORT));
      createFiles();
      TachyonTest(true);
    } else if (testCaseNumber == 2) {
      RESULT_PREFIX = "TachyonFilesReadTest " + RESULT_PREFIX;
      LOG.info(RESULT_PREFIX);
      MTC = TachyonClient.getClient(new InetSocketAddress(MASTER_HOST, Config.MASTER_PORT));
      TachyonTest(false);
    } else if (testCaseNumber == 3) {
      RESULT_PREFIX = "RamFile Write " + RESULT_PREFIX;
      LOG.info(RESULT_PREFIX);
      memoryCopyTest(true, false);
    } else if (testCaseNumber == 4) {
      RESULT_PREFIX = "RamFile Read " + RESULT_PREFIX;
      LOG.info(RESULT_PREFIX);
      memoryCopyTest(false, false);
    } else if (testCaseNumber == 5) {
      RESULT_PREFIX = "ByteBuffer Write Test " + RESULT_PREFIX;
      LOG.info(RESULT_PREFIX);
      memoryCopyTest(true, true);
    } else if (testCaseNumber == 6) {
      RESULT_PREFIX = "ByteBuffer Read Test " + RESULT_PREFIX;
      LOG.info(RESULT_PREFIX);
      memoryCopyTest(false, true);
    } else {
      CommonUtils.runtimeException("No Test Case " + testCaseNumber);
    }

    for (int k = 0; k < RESULT_ARRAY_SIZE; k ++) {
      System.out.print(Results[k] + " ");
    }
    System.out.println();
  }
}