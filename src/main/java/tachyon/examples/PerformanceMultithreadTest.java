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
import tachyon.client.Partition;
import tachyon.client.Dataset;
import tachyon.client.TachyonClient;
import tachyon.thrift.OutOfMemoryForPinDatasetException;
import tachyon.thrift.SuspectedPartitionSizeException;

public class PerformanceMultithreadTest {
  private static Logger LOG = LoggerFactory.getLogger(PerformanceMultithreadTest.class);

  private static TachyonClient MTC = null;
  private static String MASTER_HOST = null;
  private static String Dataset_NAME = null;
  private static int WRITE_BLOCK_SIZE_BYTES = -1;
  private static int BLOCKS_PER_PARTITION = -1;
  private static int THREADS = -1;
  private static int PARTITIONS = -1;
  private static boolean DEBUG_MODE = false;
  private static int PARTITION_BYTES = -1;
  private static long Dataset_BYTES = -1;
  private static int Tachyon_WRITE_BUFFER_SIZE_BYTES = -1;
  private static String RESULT_PREFIX = null;

  public static void createDataset() {
    long startTimeMs = CommonUtils.getCurrentMs();
    int datasetId = MTC.createDataset(Dataset_NAME, PARTITIONS);
    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "user_createDataset with datasetId " + datasetId);
  }

  public static class MemoryCopyWorker extends Thread {
    private int mWorkerId;
    private int mLeft;
    private int mRight;
    private ByteBuffer mBuf;
    private boolean mOneToMany;

    public MemoryCopyWorker(int id, int left, int right, ByteBuffer buf, boolean oneToMany) {
      mWorkerId = id;
      mLeft = left;
      mRight = right;
      mBuf = buf;
      mOneToMany = oneToMany;
    }

    public void memoryCopyParition() throws IOException {
      if (DEBUG_MODE) {
        mBuf.flip();
        CommonUtils.printByteBuffer(LOG, mBuf);
      }
      mBuf.flip();

      if (mOneToMany) {
        ByteBuffer dst = null; 
        for (int times = mLeft; times < mRight; times ++) {
          long startTimeMs = System.currentTimeMillis();

          if (times == mLeft) {
            dst = ByteBuffer.allocateDirect(PARTITION_BYTES);
            dst.order(ByteOrder.nativeOrder());
          }
          for (int k = 0; k < BLOCKS_PER_PARTITION; k ++) {
            mBuf.array()[0] = (byte) (k + mWorkerId);
            dst.put(mBuf.array());
          }
          long takenTimeMs = System.currentTimeMillis() - startTimeMs;
          double result = 1000L * PARTITION_BYTES / takenTimeMs / 1024 / 1024;
          LOG.info(times + "th MemCopy @ Worker " + mWorkerId + " : " + result + " Mb/sec. Took " +
              takenTimeMs + " ms. Is One To Many: " + mOneToMany);
          dst.clear();
        }
      } else {
        for (int times = mLeft; times < mRight; times ++) {
          long startTimeMs = System.currentTimeMillis();
          RandomAccessFile file =
              new RandomAccessFile(Config.WORKER_DATA_FOLDER + "1-" + times, "r");
          ByteBuffer dst = file.getChannel().map(MapMode.READ_ONLY, 0, PARTITION_BYTES);
          dst.order(ByteOrder.nativeOrder());

          for (int k = 0; k < BLOCKS_PER_PARTITION; k ++) {
            dst.get(mBuf.array());
          }
          long takenTimeMs = System.currentTimeMillis() - startTimeMs;
          double result = 1000L * PARTITION_BYTES / takenTimeMs / 1024 / 1024;
          LOG.info(times + "th MemCopy @ Worker " + mWorkerId + " : " + result + " Mb/sec. Took " +
              takenTimeMs + " ms. Is One To Many: " + mOneToMany);
          dst.clear();
        }
      }
    }

    @Override
    public void run() {
      try {
        memoryCopyParition();
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
    private TachyonClient mTC;

    public WriterWorker(int id, int left, int right, ByteBuffer buf) {
      mWorkerId = id;
      mLeft = left;
      mRight = right;
      mBuf = buf;
      mTC = TachyonClient.createTachyonClient(
          new InetSocketAddress(MASTER_HOST, Config.MASTER_PORT));
    }

    public void writeParition() throws IOException, SuspectedPartitionSizeException, TException {
      Dataset dataset = mTC.getDataset(Dataset_NAME);

      if (DEBUG_MODE) {
        mBuf.flip();
        CommonUtils.printByteBuffer(LOG, mBuf);
      }

      mBuf.flip();
      for (int pId = mLeft; pId < mRight; pId ++) {
        Partition partition = dataset.getPartition(pId);
        partition.open("w");
        long startTimeMs = System.currentTimeMillis();
        for (int k = 0; k < BLOCKS_PER_PARTITION; k ++) {
          mBuf.array()[0] = (byte) (k + mWorkerId);
          try {
            partition.append(mBuf);
          } catch (OutOfMemoryForPinDatasetException e) {
            CommonUtils.runtimeException(e);
          }
        }
        partition.close();
        long takenTimeMs = System.currentTimeMillis() - startTimeMs + 1;
        double result = WRITE_BLOCK_SIZE_BYTES * 1000L * BLOCKS_PER_PARTITION / takenTimeMs / 1024 / 1024;
        LOG.info(pId + "th Write @ Worker " + mWorkerId + ": " + result + " Mb/sec.");
      }
    }

    @Override
    public void run() {
      try {
        writeParition();
      } catch (IOException e) {
        CommonUtils.runtimeException(e);
      } catch (SuspectedPartitionSizeException e) {
        CommonUtils.runtimeException(e);
      } catch (TException e) {
        CommonUtils.runtimeException(e);
      }
      LOG.info("WriteWorker " + mWorkerId + " just finished.");
    }
  }

  public static class ReadWorker extends Thread {
    private int mWorkerId;
    private int mLeft;
    private int mRight;
    private TachyonClient mTC;

    public ReadWorker(int id, int left, int right) {
      mWorkerId = id;
      mLeft = left;
      mRight = right;
      mTC = TachyonClient.createTachyonClient(
          new InetSocketAddress(MASTER_HOST, Config.MASTER_PORT));
    }

    public void readPartition() throws IOException, SuspectedPartitionSizeException, TException {
      Dataset dataset = mTC.getDataset(Dataset_NAME);

      ByteBuffer buf;
      if (DEBUG_MODE) {
        LOG.info("Verifying the reading data...");

        for (int pId = mLeft; pId < mRight; pId ++) {
          Partition partition = dataset.getPartition(pId);
          partition.open("r");

          long startTimeMs = System.currentTimeMillis();
          buf = partition.readByteBuffer();
          IntBuffer intBuf;
          intBuf = buf.asIntBuffer();
          int tmp;
          for (int i = 0; i < BLOCKS_PER_PARTITION; i ++) {
            for (int k = 0; k < WRITE_BLOCK_SIZE_BYTES / 4; k ++) {
              tmp = intBuf.get();
              if ((k == 0 && tmp == (i + mWorkerId)) || (k != 0 && tmp == k)) {
                System.out.print(" " + tmp);
              } else {
                CommonUtils.runtimeException("WHAT? " + tmp + " " + k);
              }
            }
            System.out.println();
          }

          long takenTimeMs = System.currentTimeMillis() - startTimeMs + 1;
          double result = WRITE_BLOCK_SIZE_BYTES * 1000L * BLOCKS_PER_PARTITION / takenTimeMs / 1024 / 1024;
          LOG.info("Read Performance: " + result + " Mb/sec");
          LOG.info("Verifying read data: " + buf + " took " + takenTimeMs + " ms");
        }
      }

      for (int pId = mLeft; pId < mRight; pId ++) {
        Partition partition = dataset.getPartition(pId);
        partition.open("r");

        int[] readArray = new int[WRITE_BLOCK_SIZE_BYTES / 4];
        long startTimeMs = System.currentTimeMillis();
        buf = partition.readByteBuffer();
        IntBuffer intBuf;
        intBuf = buf.asIntBuffer();
        int tmp = 0;
        for (int i = 0; i < BLOCKS_PER_PARTITION; i ++) {
          intBuf.get(readArray);
          tmp = readArray[0];
        }
        partition.close();
        long takenTimeMs = System.currentTimeMillis() - startTimeMs + 1;
        double result = 1000L * WRITE_BLOCK_SIZE_BYTES * BLOCKS_PER_PARTITION / takenTimeMs / 1024 / 1024;

        LOG.info(pId + "th Read @ Worker " + mWorkerId + " : " + result + 
            " Mb/sec, took " + takenTimeMs + " ms. " + tmp);
        if (DEBUG_MODE) {
          buf.flip();
          CommonUtils.printByteBuffer(LOG, buf);
        }
      }
    }

    @Override
    public void run() {
      try {
        readPartition();
      } catch (IOException e) {
        CommonUtils.runtimeException(e);
      } catch (SuspectedPartitionSizeException e) {
        CommonUtils.runtimeException(e);
      } catch (TException e) {
        CommonUtils.runtimeException(e);
      }
      LOG.info("ReadWorker " + mWorkerId + " just finished.");
    }
  }

  private static void memoryCopyTest(boolean OneToMany) {
    ByteBuffer[] bufs = new ByteBuffer[THREADS];

    for (int thread = 0; thread < THREADS; thread ++) {
      ByteBuffer sRawData = ByteBuffer.allocate(WRITE_BLOCK_SIZE_BYTES);
      sRawData.order(ByteOrder.nativeOrder());
      for (int k = 0; k < WRITE_BLOCK_SIZE_BYTES / 4; k ++) {
        sRawData.putInt(k);
      }
      bufs[thread] = sRawData;
    }

    MemoryCopyWorker[] WWs = new MemoryCopyWorker[THREADS];
    int t = PARTITIONS / THREADS;
    for (int thread = 0; thread < THREADS; thread ++) {
      WWs[thread] = new MemoryCopyWorker(
          thread, t * thread, t * (thread + 1), bufs[thread], OneToMany);
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
    double result = 1000L * Dataset_BYTES / takenTimeMs / 1024 / 1024;
    LOG.info(RESULT_PREFIX + "Entire memoryCopyTest: " + result + " Mb/sec. Took " + 
        takenTimeMs + " ms.");
  }

  private static void writeTest() {
    ByteBuffer[] bufs = new ByteBuffer[THREADS];

    for (int thread = 0; thread < THREADS; thread ++) {
      ByteBuffer sRawData = ByteBuffer.allocate(WRITE_BLOCK_SIZE_BYTES);
      sRawData.order(ByteOrder.nativeOrder());
      for (int k = 0; k < WRITE_BLOCK_SIZE_BYTES / 4; k ++) {
        sRawData.putInt(k);
      }
      bufs[thread] = sRawData;
    }

    WriterWorker[] WWs = new WriterWorker[THREADS];
    int t = PARTITIONS / THREADS;
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
    double result = Dataset_BYTES * 1000L / takenTimeMs / 1024 / 1024;
    LOG.info(RESULT_PREFIX + "Entire Write: " + result + " Mb/sec. Took " + takenTimeMs + " ms.");
  }

  private static void readTest() {
    ReadWorker[] WWs = new ReadWorker[THREADS];
    int t = PARTITIONS / THREADS;
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
    double result = Dataset_BYTES * 1000L / takenTimeMs / 1024 / 1024;
    LOG.info(RESULT_PREFIX + "Entire Read: " + result + " Mb/sec. Took " + takenTimeMs + " ms.");
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 9) {
      System.out.println("java -cp target/tachyon-1.0-SNAPSHOT-jar-with-dependencies.jar " +
          "tachyon.examples.PerformanceTest " + " <MasterIp> <DatasetName> " +
          "<WriteBlockSizeInBytes> <BlocksPerPartition> <TachyonWriteBufferSize> " +
          "<DebugMode:true/false> <Threads> <PartitionsPerThread> <Test Case Number>\n" +
          "1: Dataset Write Test\n" +
          "2: Dataset Read Test\n" + 
          "3: ByteBuffer Memory Copy Test One to Many \n" +
          "4: ByteBuffer Memory Copy Test Many to One \n");
      System.exit(-1);
    }

    MASTER_HOST = args[0];
    Dataset_NAME = args[1];
    WRITE_BLOCK_SIZE_BYTES = Integer.parseInt(args[2]);
    BLOCKS_PER_PARTITION = Integer.parseInt(args[3]);
    Tachyon_WRITE_BUFFER_SIZE_BYTES = Integer.parseInt(args[4]);
    if (Tachyon_WRITE_BUFFER_SIZE_BYTES != -1) {
      // TODO 
//      Config.USER_BUFFER_PER_PARTITION_BYTES = Tachyon_WRITE_BUFFER_SIZE_BYTES;
    } else {
      Tachyon_WRITE_BUFFER_SIZE_BYTES = Config.USER_BUFFER_PER_PARTITION_BYTES;
    }
    DEBUG_MODE = ("true".equals(args[5]));
    THREADS = Integer.parseInt(args[6]);
    PARTITIONS = Integer.parseInt(args[7]) * THREADS;
    int testCaseNumber = Integer.parseInt(args[8]);

    PARTITION_BYTES = BLOCKS_PER_PARTITION * WRITE_BLOCK_SIZE_BYTES;
    Dataset_BYTES = 1L * PARTITION_BYTES * PARTITIONS;

    RESULT_PREFIX = String.format("Threads %d PartitionsPerThread %d TotalPartitions %d " +
        "BLOCK_SIZE_KB %d BLOCKS_PER_PARTITION %d PARTITION_SIZE_MB %d Dataset_SIZE_MB %d " +
        "Tachyon_WRITE_BUFFER_SIZE_KB %d : ",
        THREADS, PARTITIONS / THREADS, PARTITIONS, WRITE_BLOCK_SIZE_BYTES / 1024, 
        BLOCKS_PER_PARTITION, CommonUtils.getMB(PARTITION_BYTES), CommonUtils.getMB(Dataset_BYTES),
        Tachyon_WRITE_BUFFER_SIZE_BYTES / 1024);

    if (testCaseNumber == 1) {
      RESULT_PREFIX = "DatasetWriteTest " + RESULT_PREFIX;
      LOG.info(RESULT_PREFIX);
      MTC = TachyonClient.createTachyonClient(
          new InetSocketAddress(MASTER_HOST, Config.MASTER_PORT));
      createDataset();
      writeTest();
    } else if (testCaseNumber == 2) {
      RESULT_PREFIX = "DatasetReadTest " + RESULT_PREFIX;
      LOG.info(RESULT_PREFIX);
      MTC = TachyonClient.createTachyonClient(
          new InetSocketAddress(MASTER_HOST, Config.MASTER_PORT));
      readTest();
    } else if (testCaseNumber == 3) {
      RESULT_PREFIX = "MemoryCopyTestOneToMany " + RESULT_PREFIX;
      LOG.info(RESULT_PREFIX);
      memoryCopyTest(true);
    } else if (testCaseNumber == 4) {
      RESULT_PREFIX = "MemoryCopyTestManyToOne " + RESULT_PREFIX;
      LOG.info(RESULT_PREFIX);
      memoryCopyTest(false);
    } else {
      CommonUtils.runtimeException("No Test Case " + testCaseNumber);
    }
  }
}