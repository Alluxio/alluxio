package tachyon.examples;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.Version;
import tachyon.client.TachyonByteBuffer;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.master.DependencyType;
import tachyon.util.CommonUtils;

/**
 * An example to show to how use Tachyon's API
 */
public class BasicCheckpoint {
  private static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private static TachyonFS sTachyonClient;
  private static String sFileFolder = null;
  private static int sFiles;
  private static int sNumbers = 20;
  private static boolean sPass = true;

  public static void createDependency() throws IOException {
    long startTimeMs = CommonUtils.getCurrentMs();
    List<String> children = new ArrayList<String>();
    for (int k = 0; k < sFiles; k ++) {
      children.add(sFileFolder + "/part-" + k);
    }
    List<ByteBuffer> data = new ArrayList<ByteBuffer>();
    data.add(ByteBuffer.allocate(10));
    int depId =
        sTachyonClient.createDependency(new ArrayList<String>(), children, "fake command", data,
            "BasicCheckpoint Dependency", "Tachyon Examples", "0.3",
            DependencyType.Narrow.getValue(), 512 * Constants.MB);

    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "createDependency with depId " + depId);
  }

  public static void main(String[] args) throws IOException, TException {
    if (args.length != 3) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION
          + "-jar-with-dependencies.jar "
          + "tachyon.examples.BasicCheckpoint <TachyonMasterAddress> <FileFolder> <Files>");
      System.exit(-1);
    }
    sTachyonClient = TachyonFS.get(args[0]);
    sFileFolder = args[1];
    sFiles = Integer.parseInt(args[2]);
    createDependency();
    writeFile();
    readFile();
    Utils.printPassInfo(sPass);
    System.exit(0);
  }

  public static void readFile() throws IOException {
    for (int i = 0; i < sFiles; i ++) {
      String filePath = sFileFolder + "/part-" + i;
      LOG.debug("Reading data from " + filePath);
      TachyonFile file = sTachyonClient.getFile(filePath);
      TachyonByteBuffer buf = file.readByteBuffer();
      if (buf == null) {
        file.recache();
        buf = file.readByteBuffer();
      }
      buf.DATA.order(ByteOrder.nativeOrder());
      for (int k = 0; k < sNumbers; k ++) {
        sPass = sPass && (buf.DATA.getInt() == k);
      }
      buf.close();
    }
  }

  public static void writeFile() throws IOException {
    for (int i = 0; i < sFiles; i ++) {
      String filePath = sFileFolder + "/part-" + i;
      TachyonFile file = sTachyonClient.getFile(filePath);
      OutputStream os = file.getOutStream(WriteType.ASYNC_THROUGH);

      ByteBuffer buf = ByteBuffer.allocate(80);
      buf.order(ByteOrder.nativeOrder());
      for (int k = 0; k < sNumbers; k ++) {
        buf.putInt(k);
      }
      buf.flip();
      LOG.debug("Writing data to " + filePath);
      os.write(buf.array());
      os.close();
    }
  }
}