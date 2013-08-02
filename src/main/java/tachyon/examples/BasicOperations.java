package tachyon.examples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.log4j.Logger;

import tachyon.CommonUtils;
import tachyon.Constants;
import tachyon.Version;
import tachyon.client.OutStream;
import tachyon.client.TachyonByteBuffer;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.SuspectedFileSizeException;

public class BasicOperations {
  private static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private static TachyonFS sTachyonClient;
  private static String sFilePath = null;
  private static WriteType sWriteType = null;

  public static void createFile() throws IOException {
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
    os.write(buf.array());
    os.close();
  }

  public static void readFile()
      throws SuspectedFileSizeException, InvalidPathException, IOException {
    LOG.info("Reading data...");
    TachyonFile file = sTachyonClient.getFile(sFilePath);
    TachyonByteBuffer buf = file.readByteBuffer();
    if (buf == null) {
      file.recache();
    }
    CommonUtils.printByteBuffer(LOG, buf.DATA);
    buf.close();
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
    sTachyonClient = TachyonFS.get(args[0]);
    sFilePath = args[1];
    sWriteType = WriteType.getOpType(args[2]);
    createFile();
    writeFile();
    readFile();
    System.exit(0);
  }
}