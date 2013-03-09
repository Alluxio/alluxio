package tachyon.examples;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.corba.se.impl.util.Version;

import tachyon.CommonUtils;
import tachyon.Config;
import tachyon.client.OpType;
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
    List<Byte> data = new ArrayList<Byte>(5);
    data.add((byte) -1);
    data.add((byte) -2);
    data.add((byte) -3);
    data.add((byte) -4);
    data.add((byte) -5);
    mId = sTachyonClient.createRawTable(sTablePath, 3, data);
    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "createRawTable with id " + mId);
  }

  public static void writeParition() 
      throws IOException, TableDoesNotExistException, 
      OutOfMemoryForPinFileException, InvalidPathException, TException {
    RawTable rawTable = sTachyonClient.getRawTable(sTablePath);

    for (int column = 0; column < 3; column ++) {
      RawColumn rawColumn = rawTable.getRawColumn(column);
      if (!rawColumn.createPartition(0)) {
        CommonUtils.runtimeException("Failed to create partition in table " + sTablePath + 
            " under column " + column);
      }

      TachyonFile tFile = rawColumn.getPartition(0);
      tFile.open(OpType.WRITE_CACHE);

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
    }
  }

  public static void readPartition()
      throws IOException, TableDoesNotExistException, InvalidPathException, TException {
    LOG.info("Reading data...");
    RawTable rawTable = sTachyonClient.getRawTable(mId);
    List<Byte> metadata = rawTable.getMetadata();
    LOG.info("Metadata: ");
    for (Byte b : metadata) {
      LOG.info(b + "");
    }

    for (int column = 0; column < 3; column ++) {
      RawColumn rawColumn = rawTable.getRawColumn(column);
      TachyonFile tFile = rawColumn.getPartition(0);
      tFile.open(OpType.READ_CACHE);

      ByteBuffer buf;
      buf = tFile.readByteBuffer();
      CommonUtils.printByteBuffer(LOG, buf);
      tFile.close();
    }
  }

  public static void main(String[] args)
      throws IOException, TableDoesNotExistException, OutOfMemoryForPinFileException, 
      InvalidPathException, TException {
    if (args.length != 2) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION + 
          "-jar-with-dependencies.jar " +
          "tachyon.examples.BasicRawTableTest <TachyonMasterHostName> <FilePath>");
    }
    sTachyonClient = TachyonClient.getClient(new InetSocketAddress(args[0], Config.MASTER_PORT));
    sTablePath = args[1];
    createRawTable();
    writeParition();
    readPartition();
  }
}