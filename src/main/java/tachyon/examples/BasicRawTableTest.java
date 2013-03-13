package tachyon.examples;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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

  private static final int COLS = 3;
  private static TachyonClient sTachyonClient;
  private static String sTablePath = null;
  private static int mId;
  private static OpType sWriteType = null;

  public static void createRawTable() throws InvalidPathException {
    long startTimeMs = CommonUtils.getCurrentMs();
    ByteBuffer data = ByteBuffer.allocate(12);
    data.putInt(-1);
    data.putInt(-2);
    data.putInt(-3);
    data.flip();
    mId = sTachyonClient.createRawTable(sTablePath, 3, data);
    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "createRawTable with id " + mId);
  }

  public static void writeParition() 
      throws IOException, TableDoesNotExistException, InvalidPathException, TException {
    RawTable rawTable = sTachyonClient.getRawTable(sTablePath);

    LOG.info("Writing data...");
    for (int column = 0; column < COLS; column ++) {
      RawColumn rawColumn = rawTable.getRawColumn(column);
      if (!rawColumn.createPartition(0)) {
        CommonUtils.runtimeException("Failed to create partition in table " + sTablePath + 
            " under column " + column);
      }

      TachyonFile tFile = rawColumn.getPartition(0);
      tFile.open(sWriteType);

      ByteBuffer buf = ByteBuffer.allocate(80);
      buf.order(ByteOrder.nativeOrder());
      for (int k = 0; k < 20; k ++) {
        buf.putInt(k);
      }

      buf.flip();
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
    ByteBuffer metadata = rawTable.getMetadata();
    LOG.info("Metadata: ");
    LOG.info(metadata.getInt() + " ");
    LOG.info(metadata.getInt() + " ");
    LOG.info(metadata.getInt() + " ");

    for (int column = 0; column < COLS; column ++) {
      RawColumn rawColumn = rawTable.getRawColumn(column);
      TachyonFile tFile = rawColumn.getPartition(0);
      tFile.open(OpType.READ_TRY_CACHE);

      ByteBuffer buf;
      buf = tFile.readByteBuffer();
      CommonUtils.printByteBuffer(LOG, buf);
      tFile.close();
    }
  }

  public static void main(String[] args)
      throws IOException, TableDoesNotExistException, OutOfMemoryForPinFileException, 
      InvalidPathException, TException {
    if (args.length != 3) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION + 
          "-jar-with-dependencies.jar " +
          "tachyon.examples.BasicRawTableTest <TachyonMasterHostName> <FilePath>");
    }
    sTachyonClient = TachyonClient.getClient(new InetSocketAddress(args[0], Config.MASTER_PORT));
    sTablePath = args[1];
    sWriteType = OpType.getOpType(args[2]);
    createRawTable();
    writeParition();
    readPartition();
  }
}