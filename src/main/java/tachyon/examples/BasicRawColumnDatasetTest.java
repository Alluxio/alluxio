package tachyon.examples;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.CommonUtils;
import tachyon.Config;
import tachyon.client.RCDPartition;
import tachyon.client.RawColumnDataset;
import tachyon.client.TachyonClient;
import tachyon.thrift.OutOfMemoryForPinDatasetException;
import tachyon.thrift.SuspectedPartitionSizeException;

public class BasicRawColumnDatasetTest {
  private static Logger LOG = LoggerFactory.getLogger(BasicRawColumnDatasetTest.class);

  private static TachyonClient sTachyonClient;
  private static String sDatasetPath = null;

  public static void createRawColumnDataset() {
    long startTimeMs = CommonUtils.getCurrentMs();
    int datasetId = sTachyonClient.createRawColumnDataset(sDatasetPath, 3, 5);
    CommonUtils.printTimeTakenMs(startTimeMs, LOG,
        "createRawColumnDataset with datasetId " + datasetId);
  }

  public static void writeParition() throws SuspectedPartitionSizeException, IOException {
    RawColumnDataset dataset = sTachyonClient.getRawColumnDataset(sDatasetPath);
    RCDPartition partition = dataset.getPartition(3);
    partition.open("w");

    ByteBuffer buf = ByteBuffer.allocate(80);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < 20; k ++) {
      buf.putInt(k);
    }

    buf.flip();
    LOG.info("Writing data...");
    CommonUtils.printByteBuffer(LOG, buf);

    buf.flip();
    try {
      partition.append(2, buf);
    } catch (OutOfMemoryForPinDatasetException e) {
      CommonUtils.runtimeException(e);
    }
    partition.close();
  }

  public static void readPartition() throws SuspectedPartitionSizeException, IOException {
    LOG.info("Reading data...");
    RawColumnDataset dataset = sTachyonClient.getRawColumnDataset(sDatasetPath);
    RCDPartition partition = dataset.getPartition(3);
    partition.open("r");

    ByteBuffer buf;
    try { 
      buf = partition.readByteBuffer(2);
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
          "tachyon.examples.BasicRawColumnDatasetTest <TachyonMasterHostName> <DatasetPath>");
    }
    sTachyonClient = TachyonClient.createTachyonClient(
        new InetSocketAddress(args[0], Config.MASTER_PORT));
    sDatasetPath = args[1];
    createRawColumnDataset();
    writeParition();
    readPartition();
  }
}