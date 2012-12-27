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
import tachyon.client.Partition;
import tachyon.client.Dataset;
import tachyon.client.TachyonClient;
import tachyon.thrift.OutOfMemoryForPinDatasetException;
import tachyon.thrift.SuspectedPartitionSizeException;

public class PerformanceTest {
  private static Logger LOG = LoggerFactory.getLogger(PerformanceTest.class);

  private static TachyonClient TC;
  private static String DATASET_NAME = null;
  private static int BLOCK_SIZE_BYTES = -1;
  private static int BLOCKS_PER_PARTITION = -1;
  private static int PARTITIONS = -1;
  private static boolean DEBUG_MODE = false;

  public static void createDataset() {
    long startTimeMs = CommonUtils.getCurrentMs();
    int datasetId = TC.createDataset(DATASET_NAME, PARTITIONS);
    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "user_createDataset: with datasetId " + datasetId);
  }

  public static void writeParition() throws IOException, SuspectedPartitionSizeException, 
  TException, OutOfMemoryForPinDatasetException {
    Dataset dataset = TC.getDataset(DATASET_NAME);

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
    for (int pId = 0; pId < PARTITIONS; pId ++) {
      Partition partition = dataset.getPartition(pId);
      partition.open("w");
      long startTimeMs = System.currentTimeMillis();
      for (int k = 0; k < BLOCKS_PER_PARTITION; k ++) {
        //        long localStartTimeNs = System.nanoTime();
        buf.array()[0] = (byte) (k);
        partition.append(buf);
        //        Utils.printTimeTakenNs(localStartTimeNs, LOG, String.format(
        //            "Wrote %dth %d bytes taken for Partition %d.", k + 1, BLOCK_SIZE_BYTES, pId));
      }
      partition.close();
      long takenTimeMs = System.currentTimeMillis() - startTimeMs + 1;
      CommonUtils.printTimeTakenMs(startTimeMs, LOG, "Total time taken: ");
      double result = BLOCK_SIZE_BYTES * 1000L * BLOCKS_PER_PARTITION / takenTimeMs / 1024 / 1024;
      LOG.info("Write Performance: " + result + "Mb/sec");
      LOG.info("Done write buf close partition");
    }
  }

  public static void readPartition() 
      throws IOException, SuspectedPartitionSizeException, TException {
    Dataset dataset = TC.getDataset(DATASET_NAME);

    ByteBuffer buf;
    LOG.info("Trying to read data...");
    if (DEBUG_MODE) {
      LOG.info("Verifying the reading data...");

      for (int pId = 0; pId < PARTITIONS; pId ++) {
        Partition partition = dataset.getPartition(pId);
        partition.open("r");

        long startTimeMs = System.currentTimeMillis();
        buf = partition.readByteBuffer();
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

        long takenTimeMs = System.currentTimeMillis() - startTimeMs + 1;
        double result = BLOCK_SIZE_BYTES * 1000L * BLOCKS_PER_PARTITION / takenTimeMs / 1024 / 1024;
        LOG.info("Read Performance: " + result + "Mb/sec");
        LOG.info("Verifying read data: " + buf + " took " + takenTimeMs + " ms");
      }
    }

    for (int pId = 0; pId < PARTITIONS; pId ++) {
      Partition partition = dataset.getPartition(pId);
      partition.open("r");

      int[] readArray = new int[BLOCK_SIZE_BYTES / 4];
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
      double result = BLOCK_SIZE_BYTES * 1000L * BLOCKS_PER_PARTITION / takenTimeMs / 1024 / 1024;
      LOG.info("Read Performance: " + result + "Mb/sec. " + tmp);
      LOG.info("Read data: " + buf);
      if (DEBUG_MODE) {
        buf.flip();
        CommonUtils.printByteBuffer(LOG, buf);
      }
    }
  }

  public static void main(String[] args) throws IOException, SuspectedPartitionSizeException,
  TException, OutOfMemoryForPinDatasetException {
    if (args.length != 6) {
      System.out.println("java -cp target/tachyon-1.0-SNAPSHOT-jar-with-dependencies.jar " +
          "tachyon.examples.PerformanceTest " + " <MasterIp> <DatasetName> " +
          "<BlockSizeInBytes> <BlocksPerPartition> <DebugMode:true/false> <NumberOfPartitions>");
      System.exit(-1);
    }
    TC = TachyonClient.createTachyonClient(new InetSocketAddress(args[0], Config.MASTER_PORT));
    DATASET_NAME = args[1];
    BLOCK_SIZE_BYTES = Integer.parseInt(args[2]);
    BLOCKS_PER_PARTITION = Integer.parseInt(args[3]);
    DEBUG_MODE = ("true".equals(args[4]));
    PARTITIONS = Integer.parseInt(args[5]);

    createDataset();
    writeParition();
    readPartition();
    //    checkpointDataset();
  }
}