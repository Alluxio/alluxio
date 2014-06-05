/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.examples;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.Version;
import tachyon.client.OutStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonByteBuffer;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.conf.UserConf;
import tachyon.util.CommonUtils;

public class Performance {
  private static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private static final int RESULT_ARRAY_SIZE = 64;
  private static final String FOLDER = "/mnt/ramdisk/";

  private static TachyonFS MTC = null;
  private static String MASTER_ADDRESS = null;
  private static String FILE_NAME = null;
  private static int BLOCK_SIZE_BYTES = -1;
  private static long BLOCKS_PER_FILE = -1;
  private static int THREADS = -1;
  private static int FILES = -1;
  private static boolean DEBUG_MODE = false;
  private static long FILE_BYTES = -1;
  private static long FILES_BYTES = -1;
  private static String RESULT_PREFIX = null;
  private static long[] Results = new long[RESULT_ARRAY_SIZE];
  private static int BASE_FILE_NUMBER = 0;

  private static boolean TACHYON_STREAMING_READ = false;

  public static void createFiles() throws IOException {
    long startTimeMs = CommonUtils.getCurrentMs();
    for (int k = 0; k < THREADS; k ++) {
      int fileId = MTC.createFile(FILE_NAME + (k + BASE_FILE_NUMBER));
      CommonUtils.printTimeTakenMs(startTimeMs, LOG, "user_createFiles with fileId " + fileId);
    }
  }

  public static void logPerIteration(long startTimeMs, int times, String msg, int workerId) {
    long takenTimeMs = System.currentTimeMillis() - startTimeMs;
    double result = 1000L * FILE_BYTES / takenTimeMs / 1024 / 1024;
    LOG.info(times + msg + workerId + " : " + result + " Mb/sec. Took " + takenTimeMs + " ms. ");
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
          dst = ByteBuffer.allocateDirect((int) FILE_BYTES);
        }
        for (int times = mLeft; times < mRight; times ++) {
          long startTimeMs = System.currentTimeMillis();
          if (!mMemoryOnly) {
            file = new RandomAccessFile(FOLDER + (mWorkerId + BASE_FILE_NUMBER), "rw");
            dst = file.getChannel().map(MapMode.READ_WRITE, 0, FILE_BYTES);
          }
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
          dst = ByteBuffer.allocateDirect((int) FILE_BYTES);
        }
        for (int times = mLeft; times < mRight; times ++) {
          long startTimeMs = System.currentTimeMillis();
          if (!mMemoryOnly) {
            file = new RandomAccessFile(FOLDER + (mWorkerId + BASE_FILE_NUMBER), "rw");
            dst = file.getChannel().map(MapMode.READ_WRITE, 0, FILE_BYTES);
          }
          dst.order(ByteOrder.nativeOrder());
          for (int k = 0; k < BLOCKS_PER_FILE; k ++) {
            dst.get(mBuf.array());
          }
          sum += mBuf.get(times % 16);
          dst.clear();
          if (!mMemoryOnly) {
            file.close();
          }
          logPerIteration(startTimeMs, times, str, mWorkerId);
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
    private TachyonFS mTC;

    public TachyonWriterWorker(int id, int left, int right, ByteBuffer buf) throws IOException {
      super(id, left, right, buf);
      mTC = TachyonFS.get(MASTER_ADDRESS);
    }

    public void writeParition() throws IOException {
      if (DEBUG_MODE) {
        mBuf.flip();
        CommonUtils.printByteBuffer(LOG, mBuf);
      }

      mBuf.flip();
      for (int pId = mLeft; pId < mRight; pId ++) {
        long startTimeMs = System.currentTimeMillis();
        TachyonFile file = mTC.getFile(FILE_NAME + (mWorkerId + BASE_FILE_NUMBER));
        OutStream os = file.getOutStream(WriteType.MUST_CACHE);
        for (int k = 0; k < BLOCKS_PER_FILE; k ++) {
          mBuf.array()[0] = (byte) (k + mWorkerId);
          os.write(mBuf.array());
        }
        os.close();
        logPerIteration(startTimeMs, pId, "th WriteTachyonFile @ Worker ", pId);
      }
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
    private TachyonFS mTC;

    public TachyonReadWorker(int id, int left, int right, ByteBuffer buf) throws IOException {
      super(id, left, right, buf);
      mTC = TachyonFS.get(MASTER_ADDRESS);
    }

    public void readPartition() throws IOException {
      TachyonByteBuffer buf;
      if (DEBUG_MODE) {
        LOG.info("Verifying the reading data...");

        for (int pId = mLeft; pId < mRight; pId ++) {
          TachyonFile file = mTC.getFile(FILE_NAME + mWorkerId);
          buf = file.readByteBuffer();
          IntBuffer intBuf;
          intBuf = buf.DATA.asIntBuffer();
          int tmp;
          for (int i = 0; i < BLOCKS_PER_FILE; i ++) {
            for (int k = 0; k < BLOCK_SIZE_BYTES / 4; k ++) {
              tmp = intBuf.get();
              if ((k == 0 && tmp == (i + mWorkerId)) || (k != 0 && tmp == k)) {
              } else {
                CommonUtils.runtimeException("WHAT? " + tmp + " " + k);
              }
            }
          }
          buf.close();
        }
      }

      long sum = 0;
      if (TACHYON_STREAMING_READ) {
        for (int pId = mLeft; pId < mRight; pId ++) {
          long startTimeMs = System.currentTimeMillis();
          TachyonFile file = mTC.getFile(FILE_NAME + (mWorkerId + BASE_FILE_NUMBER));
          InputStream is = file.getInStream(ReadType.CACHE);
          long len = BLOCKS_PER_FILE * BLOCK_SIZE_BYTES;

          while (len > 0) {
            int r = is.read(mBuf.array());
            len -= r;
            if (r == -1) {
              CommonUtils.runtimeException("R == -1");
            }
          }
          is.close();
          logPerIteration(startTimeMs, pId, "th ReadTachyonFile @ Worker ", pId);
        }
      } else {
        for (int pId = mLeft; pId < mRight; pId ++) {
          long startTimeMs = System.currentTimeMillis();
          TachyonFile file = mTC.getFile(FILE_NAME + (mWorkerId + BASE_FILE_NUMBER));
          buf = file.readByteBuffer();
          for (int i = 0; i < BLOCKS_PER_FILE; i ++) {
            buf.DATA.get(mBuf.array());
          }
          sum += mBuf.get(pId % 16);

          if (DEBUG_MODE) {
            buf.DATA.flip();
            CommonUtils.printByteBuffer(LOG, buf.DATA);
          }
          buf.DATA.clear();
          logPerIteration(startTimeMs, pId, "th ReadTachyonFile @ Worker ", pId);
          buf.close();
        }
      }
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

  public static class HdfsWorker extends Worker {
    private boolean mWrite;
    private String mMsg;
    private FileSystem mHdfsFs;

    public HdfsWorker(int id, int left, int right, ByteBuffer buf, boolean write, String msg)
        throws IOException {
      super(id, left, right, buf);
      mWrite = write;
      mMsg = msg;

      Configuration tConf = new Configuration();
      tConf.set("fs.default.name", FILE_NAME);
      tConf.set("fs.defaultFS", FILE_NAME);
      tConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

      tConf.set("dfs.client.read.shortcircuit", "true");
      tConf.set("dfs.domain.socket.path", "/var/lib/hadoop-hdfs/dn_socket");
      tConf.set("dfs.client.read.shortcircuit.skip.checksum", "true");

      // tConf.addResource("/root/hadoop-2.3.0/etc/hadoop/core-site.xml");
      // tConf.addResource("/root/hadoop-2.3.0/etc/hadoop/hdfs-site.xml");
      // System.loadLibrary("hdfs");
      // System.loadLibrary("hadoop");

      mHdfsFs = FileSystem.get(tConf);
    }

    public void io() throws IOException {
      if (DEBUG_MODE) {
        mBuf.flip();
        CommonUtils.printByteBuffer(LOG, mBuf);
      }
      mBuf.flip();
      long sum = 0;
      String str = "th " + mMsg + " @ Worker ";

      if (mWrite) {
        for (int times = mLeft; times < mRight; times ++) {
          long startTimeMs = System.currentTimeMillis();
          String filePath = FILE_NAME + (mWorkerId + BASE_FILE_NUMBER);
          OutputStream os = mHdfsFs.create(new Path(filePath));
          for (int k = 0; k < BLOCKS_PER_FILE; k ++) {
            mBuf.array()[0] = (byte) (k + mWorkerId);
            os.write(mBuf.array());
          }
          os.close();
          logPerIteration(startTimeMs, times, str, mWorkerId);
        }
      } else {
        for (int times = mLeft; times < mRight; times ++) {
          long startTimeMs = System.currentTimeMillis();
          String filePath = FILE_NAME + (mWorkerId + BASE_FILE_NUMBER);
          InputStream is = mHdfsFs.open(new Path(filePath));
          long len = BLOCKS_PER_FILE * BLOCK_SIZE_BYTES;

          while (len > 0) {
            int r = is.read(mBuf.array());
            len -= r;
            if (r == -1) {
              CommonUtils.runtimeException("R == -1");
            }
          }
          is.close();
          logPerIteration(startTimeMs, times, str, mWorkerId);
        }
      }
      Results[mWorkerId] = sum;
    }

    @Override
    public void run() {
      try {
        io();
      } catch (IOException e) {
        CommonUtils.runtimeException(e);
      }
      LOG.info(mMsg + mWorkerId + " just finished.");
    }
  }

  private static void memoryCopyTest(boolean write, boolean memoryOnly) {
    ByteBuffer[] bufs = new ByteBuffer[THREADS];

    for (int thread = 0; thread < THREADS; thread ++) {
      ByteBuffer sRawData = ByteBuffer.allocate(BLOCK_SIZE_BYTES);
      sRawData.order(ByteOrder.nativeOrder());
      for (int k = 0; k < BLOCK_SIZE_BYTES / 4; k ++) {
        sRawData.putInt(k);
      }
      bufs[thread] = sRawData;
    }

    String msg = (write ? "Write" : "Read") + (memoryOnly ? "_Memory " : "_RamFile ");

    GeneralWorker[] WWs = new GeneralWorker[THREADS];
    int t = FILES / THREADS;
    for (int thread = 0; thread < THREADS; thread ++) {
      WWs[thread] =
          new GeneralWorker(thread, t * thread, t * (thread + 1), bufs[thread], write, memoryOnly,
              msg);
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

    LOG.info(result + " Mb/sec. " + RESULT_PREFIX + "Entire " + msg + " Test : " + " Took "
        + takenTimeMs + " ms. Current System Time: " + System.currentTimeMillis());
  }

  private static void TachyonTest(boolean write) throws IOException {
    ByteBuffer[] bufs = new ByteBuffer[THREADS];

    for (int thread = 0; thread < THREADS; thread ++) {
      ByteBuffer sRawData = ByteBuffer.allocate(BLOCK_SIZE_BYTES);
      sRawData.order(ByteOrder.nativeOrder());
      for (int k = 0; k < BLOCK_SIZE_BYTES / 4; k ++) {
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
    LOG.info(result + " Mb/sec. " + RESULT_PREFIX + "Entire " + (write ? "Write " : "Read ")
        + " Took " + takenTimeMs + " ms. Current System Time: " + System.currentTimeMillis());
  }

  private static void HdfsTest(boolean write) throws IOException {
    ByteBuffer[] bufs = new ByteBuffer[THREADS];

    for (int thread = 0; thread < THREADS; thread ++) {
      ByteBuffer sRawData = ByteBuffer.allocate(BLOCK_SIZE_BYTES);
      sRawData.order(ByteOrder.nativeOrder());
      for (int k = 0; k < BLOCK_SIZE_BYTES / 4; k ++) {
        sRawData.putInt(k);
      }
      bufs[thread] = sRawData;
    }

    Worker[] WWs = new Worker[THREADS];
    int t = FILES / THREADS;
    String msg = (write ? "Write " : "Read ");
    for (int thread = 0; thread < THREADS; thread ++) {
      WWs[thread] = new HdfsWorker(thread, t * thread, t * (thread + 1), bufs[thread], write, msg);
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
    LOG.info(result + " Mb/sec. " + RESULT_PREFIX + "Entire " + (write ? "Write " : "Read ")
        + " Took " + takenTimeMs + " ms. Current System Time: " + System.currentTimeMillis());
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 9) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION
          + "-jar-with-dependencies.jar tachyon.examples.Performance "
          + "<MasterIp> <FileName> <WriteBlockSizeInBytes> <BlocksPerFile> "
          + "<DebugMode:true/false> <Threads> <FilesPerThread> <TestCaseNumber> "
          + "<BaseFileNumber>\n" + "1: Files Write Test\n" + "2: Files Read Test\n"
          + "3: RamFile Write Test \n" + "4: RamFile Read Test \n" + "5: ByteBuffer Write Test \n"
          + "6: ByteBuffer Read Test \n");
      System.exit(-1);
    }

    MASTER_ADDRESS = args[0];
    FILE_NAME = args[1];
    BLOCK_SIZE_BYTES = Integer.parseInt(args[2]);
    BLOCKS_PER_FILE = Long.parseLong(args[3]);
    DEBUG_MODE = ("true".equals(args[4]));
    THREADS = Integer.parseInt(args[5]);
    FILES = Integer.parseInt(args[6]) * THREADS;
    int testCase = Integer.parseInt(args[7]);
    BASE_FILE_NUMBER = Integer.parseInt(args[8]);

    FILE_BYTES = BLOCKS_PER_FILE * BLOCK_SIZE_BYTES;
    FILES_BYTES = 1L * FILE_BYTES * FILES;

    RESULT_PREFIX =
        String.format("Threads %d FilesPerThread %d TotalFiles %d "
            + "BLOCK_SIZE_KB %d BLOCKS_PER_FILE %d FILE_SIZE_MB %d "
            + "Tachyon_WRITE_BUFFER_SIZE_KB %d BaseFileNumber %d : ", THREADS, FILES / THREADS,
            FILES, BLOCK_SIZE_BYTES / 1024, BLOCKS_PER_FILE, CommonUtils.getMB(FILE_BYTES),
            UserConf.get().FILE_BUFFER_BYTES / 1024, BASE_FILE_NUMBER);

    for (int k = 0; k < 10000000; k ++) {
      // Warmup
    }

    if (testCase == 1) {
      RESULT_PREFIX = "TachyonFilesWriteTest " + RESULT_PREFIX;
      LOG.info(RESULT_PREFIX);
      MTC = TachyonFS.get(MASTER_ADDRESS);
      createFiles();
      TachyonTest(true);
    } else if (testCase == 2 || testCase == 9) {
      RESULT_PREFIX = "TachyonFilesReadTest " + RESULT_PREFIX;
      LOG.info(RESULT_PREFIX);
      MTC = TachyonFS.get(MASTER_ADDRESS);
      TACHYON_STREAMING_READ = (9 == testCase);
      TachyonTest(false);
    } else if (testCase == 3) {
      RESULT_PREFIX = "RamFile Write " + RESULT_PREFIX;
      LOG.info(RESULT_PREFIX);
      memoryCopyTest(true, false);
    } else if (testCase == 4) {
      RESULT_PREFIX = "RamFile Read " + RESULT_PREFIX;
      LOG.info(RESULT_PREFIX);
      memoryCopyTest(false, false);
    } else if (testCase == 5) {
      RESULT_PREFIX = "ByteBuffer Write Test " + RESULT_PREFIX;
      LOG.info(RESULT_PREFIX);
      memoryCopyTest(true, true);
    } else if (testCase == 6) {
      RESULT_PREFIX = "ByteBuffer Read Test " + RESULT_PREFIX;
      LOG.info(RESULT_PREFIX);
      memoryCopyTest(false, true);
    } else if (testCase == 7) {
      RESULT_PREFIX = "HdfsFilesWriteTest " + RESULT_PREFIX;
      LOG.info(RESULT_PREFIX);
      HdfsTest(true);
    } else if (testCase == 8) {
      RESULT_PREFIX = "HdfsFilesReadTest " + RESULT_PREFIX;
      LOG.info(RESULT_PREFIX);
      HdfsTest(false);
    } else {
      CommonUtils.runtimeException("No Test Case " + testCase);
    }

    for (int k = 0; k < RESULT_ARRAY_SIZE; k ++) {
      System.out.print(Results[k] + " ");
    }
    System.out.println();
    System.exit(0);
  }
}