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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel.MapMode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.client.ClientContext;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.FileSystem;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.util.CommonUtils;
import tachyon.util.FormatUtils;

/**
 * Example to show the performance of Tachyon.
 */
public class Performance {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final int RESULT_ARRAY_SIZE = 64;
  private static final String FOLDER = "/mnt/ramdisk/";

  private static FileSystem sFileSystem = null;
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

  /**
   * Creates the files for this example.
   *
   * @throws TachyonException if creating a file fails
   * @throws IOException if a non-Tachyon related exception occurs
   */
  public static void createFiles() throws TachyonException, IOException {
    final long startTimeMs = CommonUtils.getCurrentMs();
    for (int k = 0; k < sFiles; k ++) {
      sFileSystem.createFile(new TachyonURI(sFileName + (k + sBaseFileNumber))).close();
      LOG.info(FormatUtils.formatTimeTakenMs(startTimeMs, "createFile"));
    }
  }

  /**
   * Write log information.
   *
   * @param startTimeMs the start time in milliseconds
   * @param times the number of the iteration
   * @param msg the message
   * @param workerId the id of the worker
   */
  public static void logPerIteration(long startTimeMs, int times, String msg, int workerId) {
    long takenTimeMs = System.currentTimeMillis() - startTimeMs;
    double result = 1000.0 * sFileBytes / takenTimeMs / 1024 / 1024;
    LOG.info(times + msg + workerId + " : " + result + " Mb/sec. Took " + takenTimeMs + " ms. ");
  }

  /**
   * Base class for workers used in this example.
   */
  public abstract static class Worker extends Thread {
    protected int mWorkerId;
    protected int mLeft;
    protected int mRight;
    protected ByteBuffer mBuf;

    /**
     * @param id the id of the worker
     * @param left the id of the worker on the left
     * @param right the id of the worker on the right
     * @param buf the buffer used by the worker
     */
    public Worker(int id, int left, int right, ByteBuffer buf) {
      mWorkerId = id;
      mLeft = left;
      mRight = right;
      mBuf = buf;
    }
  }

  /**
   * A general worker.
   */
  public static class GeneralWorker extends Worker {
    private boolean mOneToMany;
    private boolean mMemoryOnly;
    private String mMsg;

    /**
     * @param id the id of the worker
     * @param left the id of the worker on the left
     * @param right the id of the worker on the right
     * @param buf the buffered used by the worker
     * @param oneToMany true if the message should be written, false if it should be read
     * @param memoryOnly true if the data should reside in memory, false otherwise
     * @param msg the message to use
     */
    public GeneralWorker(int id, int left, int right, ByteBuffer buf, boolean oneToMany,
        boolean memoryOnly, String msg) {
      super(id, left, right, buf);
      mOneToMany = oneToMany;
      mMemoryOnly = memoryOnly;
      mMsg = msg;
    }

    /**
     * Copies a partition in memory.
     *
     * @throws IOException if a non-Tachyon related exception occurs
     */
    public void memoryCopyPartition() throws IOException {
      if (sDebugMode) {
        mBuf.flip();
        LOG.info(FormatUtils.byteBufferToString(mBuf));
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
        memoryCopyPartition();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
      LOG.info(mMsg + mWorkerId + " just finished.");
    }
  }

  /**
   * A worker in Tachyon for write operations.
   */
  public static class TachyonWriterWorker extends Worker {
    private FileSystem mFileSystem;

    /**
     * @param id the id of the worker
     * @param left the id of the worker on the left
     * @param right the id of the worker on the right
     * @param buf the buffer to write
     * @throws IOException if a non-Tachyon related exception occurs
     */
    public TachyonWriterWorker(int id, int left, int right, ByteBuffer buf) throws IOException {
      super(id, left, right, buf);
      mFileSystem = FileSystem.Factory.get();
    }

    /**
     * Writes a partition.
     *
     * @throws IOException if a non-Tachyon related exception occurs
     * @throws TachyonException if the write stream cannot be retrieved
     */
    public void writePartition()
            throws IOException, TachyonException {
      if (sDebugMode) {
        mBuf.flip();
        LOG.info(FormatUtils.byteBufferToString(mBuf));
      }

      mBuf.flip();
      for (int pId = mLeft; pId < mRight; pId ++) {
        final long startTimeMs = System.currentTimeMillis();
        FileOutStream os =
            mFileSystem.createFile(new TachyonURI(sFileName + (pId + sBaseFileNumber)));
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
        writePartition();
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
      LOG.info("WriteWorker " + mWorkerId + " just finished.");
    }
  }

  /**
   * A worker in Tachyon for read operations.
   */
  public static class TachyonReadWorker extends Worker {
    private FileSystem mFileSystem;

    /**
     * @param id the id of the worker
     * @param left the id of the worker on the left
     * @param right the id of the worker on the right
     * @param buf the buffer to read
     * @throws IOException if a non-Tachyon related exception occurs
     */
    public TachyonReadWorker(int id, int left, int right, ByteBuffer buf) throws IOException {
      super(id, left, right, buf);
      mFileSystem = FileSystem.Factory.get();
    }

    /**
     * Reads a partition.
     *
     * @throws IOException if a non-Tachyon related exception occurs
     * @throws TachyonException if the file cannot be opened or the stream cannot be retrieved
     */
    public void readPartition()
            throws IOException, TachyonException {
      if (sDebugMode) {
        ByteBuffer buf = ByteBuffer.allocate(sBlockSizeBytes);
        LOG.info("Verifying the reading data...");

        for (int pId = mLeft; pId < mRight; pId ++) {
          InputStream is =
              mFileSystem.openFile(new TachyonURI(sFileName + (pId + sBaseFileNumber)));
          is.read(buf.array());
          buf.order(ByteOrder.nativeOrder());
          for (int i = 0; i < sBlocksPerFile; i ++) {
            for (int k = 0; k < sBlockSizeBytes / 4; k ++) {
              int tmp = buf.getInt();
              if ((k == 0 && tmp == (i + mWorkerId)) || (k != 0 && tmp == k)) {
                LOG.debug("Partition at {} is {}", k, tmp);
              } else {
                throw new IllegalStateException("WHAT? " + tmp + " " + k);
              }
            }
          }
          is.close();
        }
      }

      long sum = 0;
      if (sTachyonStreamingRead) {
        for (int pId = mLeft; pId < mRight; pId ++) {
          final long startTimeMs = System.currentTimeMillis();
          InputStream is =
              mFileSystem.openFile(new TachyonURI(sFileName + (pId + sBaseFileNumber)));
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
          InputStream is =
              mFileSystem.openFile(new TachyonURI(sFileName + (pId + sBaseFileNumber)));
          for (int i = 0; i < sBlocksPerFile; i ++) {
            is.read(mBuf.array());
          }
          sum += mBuf.get(pId % 16);

          if (sDebugMode) {
            mBuf.order(ByteOrder.nativeOrder());
            mBuf.flip();
            LOG.info(FormatUtils.byteBufferToString(mBuf));
          }
          mBuf.clear();
          logPerIteration(startTimeMs, pId, "th ReadTachyonFile @ Worker ", pId);
          is.close();
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

  /**
   * A worker for HDFS.
   */
  public static class HdfsWorker extends Worker {
    private boolean mWrite;
    private String mMsg;
    private org.apache.hadoop.fs.FileSystem mHdfsFs;

    /**
     * @param id the id of the worker
     * @param left the id of the worker on the left
     * @param right the id of the worker on the right
     * @param buf the buffer
     * @param write indicates if data is written to HDFS
     * @param msg the message to write
     * @throws IOException if a non-Tachyon related exception occurs
     */
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

      mHdfsFs = org.apache.hadoop.fs.FileSystem.get(tConf);
    }

    /**
     * Creates IO utilization.
     *
     * @throws IOException if a non-Tachyon related exception occurs
     */
    public void io() throws IOException {
      if (sDebugMode) {
        mBuf.flip();
        LOG.info(FormatUtils.byteBufferToString(mBuf));
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
      workerThreads[thread] = new GeneralWorker(thread, t * thread, t * (thread + 1), bufs[thread],
          write, memoryOnly, msg);
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
        workerThreads[thread] =
            new TachyonWriterWorker(thread, t * thread, t * (thread + 1), bufs[thread]);
      } else {
        workerThreads[thread] =
            new TachyonReadWorker(thread, t * thread, t * (thread + 1), bufs[thread]);
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
      workerThreads[thread] =
          new HdfsWorker(thread, t * thread, t * (thread + 1), bufs[thread], write, msg);
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

  /**
   * Usage:
   * {@code java -cp <TACHYON-VERSION> tachyon.examples.Performance <MasterIp> <FileNamePrefix>
   * <WriteBlockSizeInBytes> <BlocksPerFile> <DebugMode:true/false> <Threads> <FilesPerThread>
   * <TestCaseNumber> <BaseFileNumber>}
   *
   * @param args the arguments for this example
   * @throws Exception if the example fails
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 9) {
      System.out.println("java -cp " + Version.TACHYON_JAR
          + " tachyon.examples.Performance "
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

    TachyonConf tachyonConf = ClientContext.getConf();

    long fileBufferBytes = tachyonConf.getBytes(Constants.USER_FILE_BUFFER_BYTES);
    sResultPrefix = String.format(
        "Threads %d FilesPerThread %d TotalFiles %d "
            + "BLOCK_SIZE_KB %d BLOCKS_PER_FILE %d FILE_SIZE_MB %d "
            + "Tachyon_WRITE_BUFFER_SIZE_KB %d BaseFileNumber %d : ",
        sThreads, sFiles / sThreads, sFiles, sBlockSizeBytes / 1024, sBlocksPerFile,
        sFileBytes / Constants.MB, fileBufferBytes / 1024, sBaseFileNumber);

    CommonUtils.warmUpLoop();

    tachyonConf.set(Constants.MASTER_HOSTNAME, sMasterAddress.getHost());
    tachyonConf.set(Constants.MASTER_RPC_PORT, Integer.toString(sMasterAddress.getPort()));

    if (testCase == 1) {
      sResultPrefix = "TachyonFilesWriteTest " + sResultPrefix;
      LOG.info(sResultPrefix);
      sFileSystem = FileSystem.Factory.get();
      createFiles();
      TachyonTest(true);
    } else if (testCase == 2 || testCase == 9) {
      sResultPrefix = "TachyonFilesReadTest " + sResultPrefix;
      LOG.info(sResultPrefix);
      sFileSystem = FileSystem.Factory.get();
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
