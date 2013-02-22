package tachyon.examples;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.CommonUtils;
import tachyon.Config;
import tachyon.client.RawColumn;
import tachyon.client.RawTable;
import tachyon.client.TachyonClient;
import tachyon.client.TachyonFile;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.OutOfMemoryForPinFileException;
import tachyon.thrift.TableDoesNotExistException;

public class BasicRawTableTest {
  private static Logger LOG = LoggerFactory.getLogger(BasicRawTableTest.class);

  private static TachyonClient sTachyonClient;
  private static String sTablePath = null;
  private static int mId;

  public static void createRawTable() throws InvalidPathException {
    long startTimeMs = CommonUtils.getCurrentMs();
    mId = sTachyonClient.createRawTable(sTablePath, 3);
    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "createRawTable with id " + mId);
  }

  public static void writeParition() 
      throws IOException, TableDoesNotExistException, 
      OutOfMemoryForPinFileException, InvalidPathException, TException {
    RawTable rawTable = sTachyonClient.getRawTable(sTablePath);
    RawColumn rawColumn = rawTable.getRawColumn(2);
    if (!rawColumn.createPartition(0)) {
      CommonUtils.runtimeException("Failed to create partition 2 in table " + sTablePath);
    }

    TachyonFile tFile = rawColumn.getPartition(0);
    tFile.open("w");

    ByteBuffer buf = ByteBuffer.allocate(80);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < 20; k ++) {
      buf.putInt(k);
    }

    buf.flip();
    LOG.info("Writing data...");
    CommonUtils.printByteBuffer(LOG, buf);

    buf.flip();
    tFile.append(buf);
    tFile.close();
    
    rawTable = sTachyonClient.getRawTable(mId);
    rawColumn = rawTable.getRawColumn(1);
    if (!rawColumn.createPartition(0)) {
      CommonUtils.runtimeException("Failed to create partition 2 in table " + sTablePath);
    }

    tFile = rawColumn.getPartition(0);
    tFile.open("w");

    buf = ByteBuffer.allocate(80);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 20; k < 40; k ++) {
      buf.putInt(k);
    }

    buf.flip();
    LOG.info("Writing data...");
    CommonUtils.printByteBuffer(LOG, buf);

    buf.flip();
    tFile.append(buf);
    tFile.close();
  }

  public static void readPartition()
      throws IOException, TableDoesNotExistException, InvalidPathException, TException {
    LOG.info("Reading data...");
    RawTable rawTable = sTachyonClient.getRawTable(mId);
    RawColumn rawColumn = rawTable.getRawColumn(1);
    TachyonFile tFile = rawColumn.getPartition(0);
    tFile.open("r");

    ByteBuffer buf;
    buf = tFile.readByteBuffer();
    CommonUtils.printByteBuffer(LOG, buf);
    tFile.close();
    
    rawTable = sTachyonClient.getRawTable(sTablePath);
    rawColumn = rawTable.getRawColumn(2);
    tFile = rawColumn.getPartition(0);
    tFile.open("r");

    buf = tFile.readByteBuffer();
    CommonUtils.printByteBuffer(LOG, buf);
    tFile.close();
  }

  public static void main(String[] args)
      throws IOException, TableDoesNotExistException, OutOfMemoryForPinFileException, 
      InvalidPathException, TException {
    if (args.length != 2) {
      System.out.println("java -cp target/tachyon-1.0-SNAPSHOT-jar-with-dependencies.jar " +
          "tachyon.examples.BasicRawTableTest <TachyonMasterHostName> <FilePath>");
    }
    sTachyonClient = TachyonClient.getClient(new InetSocketAddress(args[0], Config.MASTER_PORT));
    sTablePath = args[1];
    createRawTable();
    writeParition();
    readPartition();
  }
}