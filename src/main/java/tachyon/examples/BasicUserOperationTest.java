package tachyon.examples;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Config;
import tachyon.CommonUtils;
import tachyon.client.TachyonClient;
import tachyon.client.TachyonFile;
import tachyon.thrift.OutOfMemoryForPinFileException;
import tachyon.thrift.SuspectedFileSizeException;

public class BasicUserOperationTest {
  private static Logger LOG = LoggerFactory.getLogger(BasicUserOperationTest.class);

  private static TachyonClient sTachyonClient;
  private static String sFilePath = null;

  public static void createFile() {
    long startTimeMs = CommonUtils.getCurrentMs();
    int fileId = sTachyonClient.createFile(sFilePath);
    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "createFile with fileId " + fileId);
  }

  public static void writeFile() throws SuspectedFileSizeException, IOException {
    TachyonFile file = sTachyonClient.getFile(sFilePath);
    file.open("w");

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
      file.append(buf);
    } catch (OutOfMemoryForPinFileException e) {
      CommonUtils.runtimeException(e);
    }
    file.close();
  }

  public static void readFile() throws SuspectedFileSizeException, IOException {
    LOG.info("Reading data...");
    TachyonFile file = sTachyonClient.getFile(sFilePath);
    file.open("r");

    ByteBuffer buf;
    try { 
      buf = file.readByteBuffer();
      CommonUtils.printByteBuffer(LOG, buf);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      CommonUtils.runtimeException(e);
    }

    file.close();
  }

  public static void main(String[] args) throws SuspectedFileSizeException, IOException {
    if (args.length != 2) {
      System.out.println("java -cp target/tachyon-1.0-SNAPSHOT-jar-with-dependencies.jar " +
          "tachyon.examples.BasicUserOperationTest <TachyonMasterHostName> <FilePath>");
    }
    sTachyonClient = TachyonClient.getClient(
        new InetSocketAddress(args[0], Config.MASTER_PORT));
    sFilePath = args[1];
    createFile();
    writeFile();
    readFile();
  }
}