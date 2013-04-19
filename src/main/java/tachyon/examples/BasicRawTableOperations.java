package tachyon.examples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.thrift.TException; 
import org.apache.log4j.Logger;

import tachyon.CommonUtils;
import tachyon.Constants;
import tachyon.Version;
import tachyon.client.OutStream;
import tachyon.client.OpType;
import tachyon.client.RawColumn;
import tachyon.client.RawTable;
import tachyon.client.TachyonClient;
import tachyon.client.TachyonFile;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.OutOfMemoryForPinFileException;
import tachyon.thrift.TableDoesNotExistException;

public class BasicRawTableOperations {
  private static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

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
      throws IOException, TableDoesNotExistException, InvalidPathException, 
      FileAlreadyExistException, TException {
    RawTable rawTable = sTachyonClient.getRawTable(sTablePath);

    LOG.info("Writing data...");
    for (int column = 0; column < COLS; column ++) {
      RawColumn rawColumn = rawTable.getRawColumn(column);
      if (!rawColumn.createPartition(0)) {
        CommonUtils.runtimeException("Failed to create partition in table " + sTablePath + 
            " under column " + column);
      }

      ByteBuffer buf = ByteBuffer.allocate(80);
      buf.order(ByteOrder.nativeOrder());
      for (int k = 0; k < 20; k ++) {
        buf.putInt(k);
      }
      buf.flip();
      CommonUtils.printByteBuffer(LOG, buf);
      buf.flip();

      TachyonFile tFile = rawColumn.getPartition(0);
      OutStream os = tFile.createOutStream(sWriteType);
      os.write(buf);
      os.close();
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

      ByteBuffer buf = tFile.readByteBuffer();
      if (buf == null) {
        tFile.recacheData();
      }
      CommonUtils.printByteBuffer(LOG, tFile.readByteBuffer());
      tFile.releaseFileLock();
    }
  }

  public static void main(String[] args)
      throws IOException, TableDoesNotExistException, OutOfMemoryForPinFileException, 
      InvalidPathException, FileAlreadyExistException, TException {
    if (args.length != 3) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION + 
          "-jar-with-dependencies.jar " +
          "tachyon.examples.BasicRawTableOperations <TachyonMasterAddress> <FilePath>");
      System.exit(-1);
    }
    sTachyonClient = TachyonClient.getClient(args[0]);
    sTablePath = args[1];
    sWriteType = OpType.getOpType(args[2]);
    createRawTable();
    writeParition();
    readPartition();
  }
}