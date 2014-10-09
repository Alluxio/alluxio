package tachyon.examples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.client.OutStream;
import tachyon.client.TachyonByteBuffer;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.util.CommonUtils;

public class BasicOperations implements Callable<Boolean> {
  private static Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonURI mMasterLocation;
  private final TachyonURI mFilePath;
  private final WriteType sWriteType;
  private final int mNumbers = 20;

  public BasicOperations(TachyonURI mMasterLocation, TachyonURI mFilePath, WriteType sWriteType) {
    this.mMasterLocation = mMasterLocation;
    this.mFilePath = mFilePath;
    this.sWriteType = sWriteType;
  }

  @Override
  public Boolean call() throws Exception {
    TachyonFS tachyonClient = TachyonFS.get(mMasterLocation);
    createFile(tachyonClient);
    writeFile(tachyonClient);
    return readFile(tachyonClient);
  }

  private void createFile(TachyonFS tachyonClient) throws IOException {
    LOG.debug("Creating file...");
    long startTimeMs = CommonUtils.getCurrentMs();
    int fileId = tachyonClient.createFile(mFilePath);
    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "createFile with fileId " + fileId);
  }

  private void writeFile(TachyonFS tachyonClient) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(mNumbers * 4);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < mNumbers; k ++) {
      buf.putInt(k);
    }

    buf.flip();
    LOG.debug("Writing data...");
    buf.flip();

    long startTimeMs = CommonUtils.getCurrentMs();
    TachyonFile file = tachyonClient.getFile(mFilePath);
    OutStream os = file.getOutStream(sWriteType);
    os.write(buf.array());
    os.close();

    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "writeFile to file " + mFilePath);
  }

  private boolean readFile(TachyonFS tachyonClient) throws IOException {
    boolean pass = true;
    LOG.debug("Reading data...");

    long startTimeMs = CommonUtils.getCurrentMs();
    TachyonFile file = tachyonClient.getFile(mFilePath);
    TachyonByteBuffer buf = file.readByteBuffer(0);
    if (buf == null) {
      file.recache();
      buf = file.readByteBuffer(0);
    }
    buf.DATA.order(ByteOrder.nativeOrder());
    for (int k = 0; k < mNumbers; k ++) {
      pass = pass && (buf.DATA.getInt() == k);
    }
    buf.close();

    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "readFile file " + mFilePath);
    return pass;
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 3) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION
          + "-jar-with-dependencies.jar "
          + "tachyon.examples.BasicOperations <TachyonMasterAddress> <FilePath> <WriteType>");
      System.exit(-1);
    };

    Utils.runExample(new BasicOperations(new TachyonURI(args[0]), new TachyonURI(args[1]),
        WriteType.getOpType(args[2])));
  }
}
