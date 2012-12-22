package tachyon.examples;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Config;
import tachyon.CommonUtils;
import tachyon.client.Partition;
import tachyon.client.Dataset;
import tachyon.client.TachyonClient;
import tachyon.thrift.OutOfMemoryForPinDatasetException;
import tachyon.thrift.SuspectedPartitionSizeException;

public class BasicUserOperationTest {
  private static Logger LOG = LoggerFactory.getLogger(BasicUserOperationTest.class);

  private static TachyonClient sTachyonClient;
  private static String sDatasetPath = null;

  public static void createDataset() {
    long startTimeMs = CommonUtils.getCurrentMs();
    int datasetId = sTachyonClient.createDataset(sDatasetPath, 5, "hdfs://");
    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "createDataset with datasetId " + datasetId);
  }

  public static void writeParition() throws SuspectedPartitionSizeException, IOException {
    Dataset dataset = sTachyonClient.getDataset(sDatasetPath);
    Partition partition = dataset.getPartition(3);
    partition.open("w");

    ByteBuffer buf = ByteBuffer.allocate(80);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < 20; k ++) {
      buf.putInt(k);
    }

    buf.flip();
    LOG.info("Done write buf init: " + buf);
    CommonUtils.printByteBuffer(LOG, buf);

    buf.flip();
    try {
      partition.append(buf);
    } catch (OutOfMemoryForPinDatasetException e) {
      CommonUtils.runtimeException(e);
    }
    LOG.info("Done write buf");
    partition.close();
    LOG.info("Done write buf close partition");
  }

  public static void readPartition() throws SuspectedPartitionSizeException, IOException {
    Dataset dataset = sTachyonClient.getDataset(sDatasetPath);
    Partition partition = dataset.getPartition(3);
    partition.open("r");

    ByteBuffer buf;
    try {
      LOG.info("Trying to read data...");
      buf = partition.readByteBuffer();

      LOG.info("Read data: " + buf);
      CommonUtils.printByteBuffer(LOG, buf);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      CommonUtils.runtimeException(e);
    }

    partition.close();
  }

  public static void main(String[] args) throws SuspectedPartitionSizeException, IOException {
    if (args.length != 2) {
      System.out.println("java -cp target/tachyon-1.0-SNAPSHOT-jar-with-dependencies.jar " +
          "tachyon.examples.BasicUserOperationTest <TachyonMasterHostName> <DatasetPath>");
    }
    sTachyonClient = TachyonClient.createTachyonClient(
        new InetSocketAddress(args[0], Config.MASTER_PORT));
    sDatasetPath = args[1];
    createDataset();
    writeParition();
    readPartition();
  }
}
