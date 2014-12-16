/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.examples;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.client.OutStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonByteBuffer;
import tachyon.client.TachyonFile;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;

public class Performance {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final int RESULT_ARRAY_SIZE = 64;
  private static final String FOLDER = "/mnt/ramdisk/";

  private static TachyonFS sMtc = null;
  private static TachyonURI sMasterAddress = null;
  private static String sFileName = null;
  private static int sBlockSizeBytes = -1;
  private static long sBlocksPerFile = -1;
  private static int sThreads = -1;
  private static int sFiles = -1;
  private static boolean sDebugMode = false;
  private static long sFileBytes = -1;
  private static long sFilesBytes = -1;
  private static String sResultPrefix = null;
  private static long[] sResults = new long[RESULT_ARRAY_SIZE];
  private static int sBaseFileNumber = 0;
  private static boolean sTachyonStreamingRead = false;

  public static void createFiles() throws IOException {
    final long startTimeMs = CommonUtils.getCurrentMs();
    for (int k = 0; k < sFiles; k ++) {
      int fileId = sMtc.createFile(new TachyonURI(sFileName + (k + sBaseFileNumber)));
      CommonUtils.printTimeTakenMs(startTimeMs, LOG, "user_createFiles with fileId " + fileId);
    }
  }

  public static void logPerIteration(long startTimeMs, int times, String msg, int workerId) {
    long takenTimeMs = System.currentTimeMillis() - startTimeMs;
    double result = 1000.0 * sFileBytes / takenTimeMs / 1024 / 1024;
    LOG.info(times + msg + workerId + " : " + result + " Mb/sec. Took " + takenTimeMs + " ms. ");
  }

  public abstract static class Worker extends Thread {
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
      if (sDebugMode) {
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
          dst = ByteBuffer.allocateDirect((int) sFileBytes);
        }
        for (int times = mLeft; times < mRight; times ++) {
          final long startTimeMs = System.currentTimeMillis();
          if (!mMemoryOnly) {
            file = new RandomAccessFile(FOLDER + (times + sBaseFileNumber), "rw");
            dst = file.getChannel().map(MapMode.READ_WRITE, 0, sFileBytes);
          }
          dst.order(ByteOrder.nativeOrder());
          for (int k = 0; k < sBlocksPerFile; k ++) {
            mBuf.putInt(0, k + mWorkerId);
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
          dst = ByteBuffer.allocateDirect((int) sFileBytes);
        }
        for (int times = mLeft; times < mRight; times ++) {
          final long startTimeMs = System.currentTimeMillis();
          if (!mMemoryOnly) {
            file = new RandomAccessFile(FOLDER + (times + sBaseFileNumber), "rw");
            dst = file.getChannel().map(MapMode.READ_WRITE, 0, sFileBytes);
          }
          dst.order(ByteOrder.nativeOrder());
          for (int k = 0; k < sBlocksPerFile; k ++) {
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
      sResults[mWorkerId] = sum;
    }

    @Override
    public void run() {
      try {
        memoryCopyParition();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
      LOG.info(mMsg + mWorkerId + " just finished.");
    }
  }

  public static class TachyonWriterWorker extends Worker {
    private TachyonFS mTC;

    public TachyonWriterWorker(int id, int left, int right, ByteBuffer buf) throws IOException {
      super(id, left, right, buf);
      mTC = TachyonFS.get(sMasterAddress, new TachyonConf());
    }

    public void writeParition() throws IOException {
      if (sDebugMode) {
        mBuf.flip();
        CommonUtils.printByteBuffer(LOG, mBuf);
      }

      mBuf.flip();
      for (int pId = mLeft; pId < mRight; pId ++) {
        final long startTimeMs = System.currentTimeMillis();
        TachyonFile file = mTC.getFile(new TachyonURI(sFileName + (pId + sBaseFileNumber)));
        OutStream os = file.getOutStream(WriteType.MUST_CACHE);
        for (int k = 0; k < sBlocksPerFile; k ++) {
          mBuf.putInt(0, k + mWorkerId);
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
        throw Throwables.propagate(e);
      }
      LOG.info("WriteWorker " + mWorkerId + " just finished.");
    }
  }

  public static class TachyonReadWorker extends Worker {
    private TachyonFS mTC;

    public TachyonReadWorker(int id, int left, int right, ByteBuffer buf) throws IOException {
      super(id, left, right, buf);
      mTC = TachyonFS.get(sMasterAddress, new TachyonConf());
    }

    public void readPartition() throws IOException {
      TachyonByteBuffer buf;
      if (sDebugMode) {
        LOG.info("Verifying the reading data...");

        for (int pId = mLeft; pId < mRight; pId ++) {
          TachyonFile file = mTC.getFile(new TachyonURI(sFileName + (pId + sBaseFileNumber)));
          buf = file.readByteBuffer(0);
          IntBuffer intBuf;
          intBuf = buf.mData.order(ByteOrder.nativeOrder()).asIntBuffer();
          for (int i = 0; i < sBlocksPerFile; i ++) {
            for (int k = 0; k < sBlockSizeBytes / 4; k ++) {
              int tmp = intBuf.get();
              if ((k == 0 && tmp == (i + mWorkerId)) || (k != 0 && tmp == k)) {
                LOG.debug("Partition at {} is {}", k, tmp);
              } else {
                throw new IllegalStateException("WHAT? " + tmp + " " + k);
              }
            }
          }
          buf.close();
        }
      }

      long sum = 0;
      if (sTachyonStreamingRead) {
        for (int pId = mLeft; pId < mRight; pId ++) {
          final long startTimeMs = System.currentTimeMillis();
          TachyonFile file = mTC.getFile(new TachyonURI(sFileName + (pId + sBaseFileNumber)));
          InputStream is = file.getInStream(ReadType.CACHE);
          long len = sBlocksPerFile * sBlockSizeBytes;

          while (len > 0) {
            int r = is.read(mBuf.array());
            len -= r;
            Preconditions.checkState(r != -1, "R == -1");
          }
          is.close();
          logPerIteration(startTimeMs, pId, "th ReadTachyonFile @ Worker ", pId);
        }
      } else {
        for (int pId = mLeft; pId < mRight; pId ++) {
          final long startTimeMs = System.currentTimeMillis();
          TachyonFile file = mTC.getFile(new TachyonURI(sFileName + (pId + sBaseFileNumber)));
          buf = file.readByteBuffer(0);
          for (int i = 0; i < sBlocksPerFile; i ++) {
            buf.mData.get(mBuf.array());
          }
          sum += mBuf.get(pId % 16);

          if (sDebugMode) {
            buf.mData.order(ByteOrder.nativeOrder()).flip();
            CommonUtils.printByteBuffer(LOG, buf.mData);
          }
          buf.mData.clear();
          logPerIteration(startTimeMs, pId, "th ReadTachyonFile @ Worker ", pId);
          buf.close();
        }
      }
      sResults[mWorkerId] = sum;
    }

    @Override
    public void run() {
      try {
        readPartition();
      } catch (Exception e) {
        throw Throwables.propagate(e);
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
      tConf.set("fs.default.name", sFileName);
      tConf.set("fs.defaultFS", sFileName);
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
      if (sDebugMode) {
        mBuf.flip();
        CommonUtils.printByteBuffer(LOG, mBuf);
      }
      mBuf.flip();
      long sum = 0;
      String str = "th " + mMsg + " @ Worker ";

      if (mWrite) {
        for (int times = mLeft; times < mRight; times ++) {
          final long startTimeMs = System.currentTimeMillis();
          String filePath = sFileName + (times + sBaseFileNumber);
          OutputStream os = mHdfsFs.create(new Path(filePath));
          for (int k = 0; k < sBlocksPerFile; k ++) {
            mBuf.putInt(0, k + mWorkerId);
            os.write(mBuf.array());
          }
          os.close();
          logPerIteration(startTimeMs, times, str, mWorkerId);
        }
      } else {
        for (int times = mLeft; times < mRight; times ++) {
          final long startTimeMs = System.currentTimeMillis();
          String filePath = sFileName + (times + sBaseFileNumber);
          InputStream is = mHdfsFs.open(new Path(filePath));
          long len = sBlocksPerFile * sBlockSizeBytes;

          while (len > 0) {
            int r = is.read(mBuf.array());
            len -= r;
            Preconditions.checkState(r != -1, "R == -1");
          }
          is.close();
          logPerIteration(startTimeMs, times, str, mWorkerId);
        }
      }
      sResults[mWorkerId] = sum;
    }

    @Override
    public void run() {
      try {
        io();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
      LOG.info(mMsg + mWorkerId + " just finished.");
    }
  }

  private static void memoryCopyTest(boolean write, boolean memoryOnly) {
    ByteBuffer[] bufs = new ByteBuffer[sThreads];

    for (int thread = 0; thread < sThreads; thread ++) {
      ByteBuffer sRawData = ByteBuffer.allocate(sBlockSizeBytes);
      sRawData.order(ByteOrder.nativeOrder());
      for (int k = 0; k < sBlockSizeBytes / 4; k ++) {
        sRawData.putInt(k);
      }
      bufs[thread] = sRawData;
    }

    String msg = (write ? "Write" : "Read") + (memoryOnly ? "_Memory " : "_RamFile ");

    GeneralWorker[] workerThreads = new GeneralWorker[sThreads];
    int t = sFiles / sThreads;
    for (int thread = 0; thread < sThreads; thread ++) {
      workerThreads[thread] =
          new GeneralWorker(thread, t * thread, t * (thread + 1), bufs[thread], write, memoryOnly,
              msg);
    }

    final long startTimeMs = System.currentTimeMillis();
    for (int thread = 0; thread < sThreads; thread ++) {
      workerThreads[thread].start();
    }
    for (int thread = 0; thread < sThreads; thread ++) {
      try {
        workerThreads[thread].join();
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }
    final long takenTimeMs = System.currentTimeMillis() - startTimeMs;
    double result = 1000.0 * sFilesBytes / takenTimeMs / 1024 / 1024;

    LOG.info(result + " Mb/sec. " + sResultPrefix + "Entire " + msg + " Test : " + " Took "
        + takenTimeMs + " ms. Current System Time: " + System.currentTimeMillis());
  }

  private static void TachyonTest(boolean write) throws IOException {
    ByteBuffer[] bufs = new ByteBuffer[sThreads];

    for (int thread = 0; thread < sThreads; thread ++) {
      ByteBuffer sRawData = ByteBuffer.allocate(sBlockSizeBytes);
      sRawData.order(ByteOrder.nativeOrder());
      for (int k = 0; k < sBlockSizeBytes / 4; k ++) {
        sRawData.putInt(k);
      }
      bufs[thread] = sRawData;
    }

    Worker[] workerThreads = new Worker[sThreads];
    int t = sFiles / sThreads;
    for (int thread = 0; thread < sThreads; thread ++) {
      if (write) {
        workerThreads[thread] = new TachyonWriterWorker(thread, t * thread, t * (thread + 1),
            bufs[thread]);
      } else {
        workerThreads[thread] = new TachyonReadWorker(thread, t * thread, t * (thread + 1),
            bufs[thread]);
      }
    }

    final long startTimeMs = System.currentTimeMillis();
    for (int thread = 0; thread < sThreads; thread ++) {
      workerThreads[thread].start();
    }
    for (int thread = 0; thread < sThreads; thread ++) {
      try {
        workerThreads[thread].join();
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }
    final long takenTimeMs = System.currentTimeMillis() - startTimeMs;
    double result = sFilesBytes * 1000.0 / takenTimeMs / 1024 / 1024;
    LOG.info(result + " Mb/sec. " + sResultPrefix + "Entire " + (write ? "Write " : "Read ")
        + " Took " + takenTimeMs + " ms. Current System Time: " + System.currentTimeMillis());
  }

  private static void HdfsTest(boolean write) throws IOException {
    ByteBuffer[] bufs = new ByteBuffer[sThreads];

    for (int thread = 0; thread < sThreads; thread ++) {
      ByteBuffer sRawData = ByteBuffer.allocate(sBlockSizeBytes);
      sRawData.order(ByteOrder.nativeOrder());
      for (int k = 0; k < sBlockSizeBytes / 4; k ++) {
        sRawData.putInt(k);
      }
      bufs[thread] = sRawData;
    }

    Worker[] workerThreads = new Worker[sThreads];
    int t = sFiles / sThreads;
    String msg = (write ? "Write " : "Read ");
    for (int thread = 0; thread < sThreads; thread ++) {
      workerThreads[thread] = new HdfsWorker(thread, t * thread, t * (thread + 1), bufs[thread],
          write, msg);
    }

    final long startTimeMs = System.currentTimeMillis();
    for (int thread = 0; thread < sThreads; thread ++) {
      workerThreads[thread].start();
    }
    for (int thread = 0; thread < sThreads; thread ++) {
      try {
        workerThreads[thread].join();
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }
    final long takenTimeMs = System.currentTimeMillis() - startTimeMs;
    double result = sFilesBytes * 1000.0 / takenTimeMs / 1024 / 1024;
    LOG.info(result + " Mb/sec. " + sResultPrefix + "Entire " + (write ? "Write " : "Read ")
        + " Took " + takenTimeMs + " ms. Current System Time: " + System.currentTimeMillis());
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 9) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION
          + "-jar-with-dependencies.jar tachyon.examples.Performance "
          + "<MasterIp> <FileNamePrefix> <WriteBlockSizeInBytes> <BlocksPerFile> "
          + "<DebugMode:true/false> <Threads> <FilesPerThread> <TestCaseNumber> "
          + "<BaseFileNumber>\n" + "1: Files Write Test\n" + "2: Files Read Test\n"
          + "3: RamFile Write Test \n" + "4: RamFile Read Test \n" + "5: ByteBuffer Write Test \n"
          + "6: ByteBuffer Read Test \n");
      System.exit(-1);
    }

    sMasterAddress = new TachyonURI(args[0]);
    sFileName = args[1];
    sBlockSizeBytes = Integer.parseInt(args[2]);
    sBlocksPerFile = Long.parseLong(args[3]);
    sDebugMode = ("true".equals(args[4]));
    sThreads = Integer.parseInt(args[5]);
    sFiles = Integer.parseInt(args[6]) * sThreads;
    final int testCase = Integer.parseInt(args[7]);
    sBaseFileNumber = Integer.parseInt(args[8]);

    sFileBytes = sBlocksPerFile * sBlockSizeBytes;
    sFilesBytes = 1L * sFileBytes * sFiles;

    TachyonConf tachyonConf = new TachyonConf();

    long fileBufferBytes = tachyonConf.getBytes(Constants.USER_FILE_BUFFER_BYTES, 0);
    sResultPrefix =
        String.format("Threads %d FilesPerThread %d TotalFiles %d "
            + "BLOCK_SIZE_KB %d BLOCKS_PER_FILE %d FILE_SIZE_MB %d "
            + "Tachyon_WRITE_BUFFER_SIZE_KB %d BaseFileNumber %d : ", sThreads, sFiles / sThreads,
            sFiles, sBlockSizeBytes / 1024, sBlocksPerFile, CommonUtils.getMB(sFileBytes),
            fileBufferBytes / 1024, sBaseFileNumber);

    for (int k = 0; k < 10000000; k ++) {
      // Warmup
    }

    if (testCase == 1) {
      sResultPrefix = "TachyonFilesWriteTest " + sResultPrefix;
      LOG.info(sResultPrefix);
      sMtc = TachyonFS.get(sMasterAddress, new TachyonConf());
      createFiles();
      TachyonTest(true);
    } else if (testCase == 2 || testCase == 9) {
      sResultPrefix = "TachyonFilesReadTest " + sResultPrefix;
      LOG.info(sResultPrefix);
      sMtc = TachyonFS.get(sMasterAddress, new TachyonConf());
      sTachyonStreamingRead = (9 == testCase);
      TachyonTest(false);
    } else if (testCase == 3) {
      sResultPrefix = "RamFile Write " + sResultPrefix;
      LOG.info(sResultPrefix);
      memoryCopyTest(true, false);
    } else if (testCase == 4) {
      sResultPrefix = "RamFile Read " + sResultPrefix;
      LOG.info(sResultPrefix);
      memoryCopyTest(false, false);
    } else if (testCase == 5) {
      sResultPrefix = "ByteBuffer Write Test " + sResultPrefix;
      LOG.info(sResultPrefix);
      memoryCopyTest(true, true);
    } else if (testCase == 6) {
      sResultPrefix = "ByteBuffer Read Test " + sResultPrefix;
      LOG.info(sResultPrefix);
      memoryCopyTest(false, true);
    } else if (testCase == 7) {
      sResultPrefix = "HdfsFilesWriteTest " + sResultPrefix;
      LOG.info(sResultPrefix);
      HdfsTest(true);
    } else if (testCase == 8) {
      sResultPrefix = "HdfsFilesReadTest " + sResultPrefix;
      LOG.info(sResultPrefix);
      HdfsTest(false);
    } else {
      throw new RuntimeException("No Test Case " + testCase);
    }

    for (int k = 0; k < RESULT_ARRAY_SIZE; k ++) {
      System.out.print(sResults[k] + " ");
    }
    System.out.println();
    System.exit(0);
  }
}
