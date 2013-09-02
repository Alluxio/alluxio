package tachyon.examples;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import tachyon.CommonUtils;
import tachyon.Constants;
import tachyon.DependencyType;
import tachyon.Version;
import tachyon.client.TachyonByteBuffer;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.SuspectedFileSizeException;

public class BasicCheckpoint {
  private static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private static TachyonFS sTachyonClient;
  private static String sFilePath = null;
  private static int sFiles;

  public static void createDependency() throws IOException {
    long startTimeMs = CommonUtils.getCurrentMs();
    List<String> children = new ArrayList<String>();
    for (int k = 0; k < sFiles; k ++) {
      children.add(sFilePath + "/part-" + k);
    }
    List<ByteBuffer> data = new ArrayList<ByteBuffer>();
    data.add(ByteBuffer.allocate(10));
    int depId = sTachyonClient.createDependency(new ArrayList<String>(),
        children, "fake command", data, "BasicCheckpoint Dependency", "Tachyon Examples", "0.3",
        DependencyType.Narrow.getValue(), 512 * Constants.MB);

    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "createDependency with depId " + depId);
  }

  public static void writeFile() throws IOException {
    for (int i = 0; i < sFiles; i ++) {
      String filePath = sFilePath + "/part-" + i;
      TachyonFile file = sTachyonClient.getFile(filePath);
      OutputStream os = file.getOutStream(WriteType.MUST_CACHE);

      ByteBuffer buf = ByteBuffer.allocate(80);
      buf.order(ByteOrder.nativeOrder());
      for (int k = 0; k < 20; k ++) {
        buf.putInt(k);
      }
      buf.flip();
      LOG.info("Writing data to " + filePath);
      CommonUtils.printByteBuffer(LOG, buf);
      buf.flip();
      os.write(buf.array());
      os.close();
    }
  }

  public static void readFile() throws IOException {
    for (int i = 0; i < sFiles; i ++) {
      String filePath = sFilePath + "/part-" + i;
      LOG.info("Reading data from " + filePath);
      TachyonFile file = sTachyonClient.getFile(filePath);
      TachyonByteBuffer buf = file.readByteBuffer();
      if (buf == null) {
        file.recache();
        buf = file.readByteBuffer();
      }
      CommonUtils.printByteBuffer(LOG, buf.DATA);
      buf.close();
    }
  }

  public static void main(String[] args)
      throws SuspectedFileSizeException, InvalidPathException, IOException, 
      FileDoesNotExistException, FileAlreadyExistException, TException {
    if (args.length != 3) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION + 
          "-jar-with-dependencies.jar " +
          "tachyon.examples.BasicCheckpoint <TachyonMasterAddress> <FilePath> <Files>");
      System.exit(-1);
    }
    sTachyonClient = TachyonFS.get(args[0]);
    sFilePath = args[1];
    sFiles = Integer.parseInt(args[2]);
    createDependency();
    writeFile();
    readFile();
  }
}