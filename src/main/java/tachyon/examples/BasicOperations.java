package tachyon.examples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.log4j.Logger;

import tachyon.CommonUtils;
import tachyon.Constants;
import tachyon.Version;
import tachyon.client.OutStream;
import tachyon.client.OpType;
import tachyon.client.TachyonClient;
import tachyon.client.TachyonFile;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.SuspectedFileSizeException;

public class BasicOperations {
  private static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private static TachyonClient sTachyonClient;
  private static String sFilePath = null;
  private static OpType sWriteType = null;

  public static void createFile() throws InvalidPathException, FileAlreadyExistException {
    long startTimeMs = CommonUtils.getCurrentMs();
    int fileId = sTachyonClient.createFile(sFilePath);
    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "createFile with fileId " + fileId);
  }

  public static void writeFile()
      throws SuspectedFileSizeException, InvalidPathException, IOException {
    ByteBuffer buf = ByteBuffer.allocate(80);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < 20; k ++) {
      buf.putInt(k);
    }

    buf.flip();
    LOG.info("Writing data...");
    CommonUtils.printByteBuffer(LOG, buf);
    buf.flip();

    TachyonFile file = sTachyonClient.getFile(sFilePath);
    OutStream os = file.getOutStream(sWriteType);
    os.write(buf);
    os.close();
  }

  public static void readFile()
      throws SuspectedFileSizeException, InvalidPathException, IOException {
    LOG.info("Reading data...");
    TachyonFile file = sTachyonClient.getFile(sFilePath);
    ByteBuffer buf = file.readByteBuffer();
    if (buf == null) {
      file.recacheData();
    }
    CommonUtils.printByteBuffer(LOG, file.readByteBuffer());
    file.releaseFileLock();
  }

  public static void main(String[] args)
      throws SuspectedFileSizeException, InvalidPathException, IOException,
      FileAlreadyExistException {
    if (args.length != 3) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION + 
          "-jar-with-dependencies.jar " +
          "tachyon.examples.BasicOperations <TachyonMasterAddress> <FilePath> <WriteType>");
      System.exit(-1);
    }
    sTachyonClient = TachyonClient.getClient(args[0]);
    sFilePath = args[1];
    sWriteType = OpType.getOpType(args[2]);
    createFile();
    writeFile();
    readFile();
  }
}