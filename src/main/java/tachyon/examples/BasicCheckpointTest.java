package tachyon.examples;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import tachyon.CommonUtils;
import tachyon.Config;
import tachyon.DependencyType;
import tachyon.Version;
import tachyon.client.OpType;
import tachyon.client.TachyonClient;
import tachyon.client.TachyonFile;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.SuspectedFileSizeException;

public class BasicCheckpointTest {
  private static Logger LOG = Logger.getLogger(Config.LOGGER_TYPE);

  private static TachyonClient sTachyonClient;
  private static String sFilePath = null;
  private static int sFiles;

  public static void createDependency() throws InvalidPathException, FileDoesNotExistException,
  FileAlreadyExistException, TException {
    long startTimeMs = CommonUtils.getCurrentMs();
    List<String> children = new ArrayList<String>();
    for (int k = 0; k < sFiles; k ++) {
      children.add(sFilePath + "/part-" + k);
    }
    List<ByteBuffer> data = new ArrayList<ByteBuffer>();
    data.add(ByteBuffer.allocate(10));
    int depId = sTachyonClient.createDependency(new ArrayList<String>(),
        children, "fake command", data,
        "BasicCheckpointTest Dependency", "Tachyon Examples", "0.2", DependencyType.Narrow);

    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "createDependency with depId " + depId);
  }

  public static void writeFile()
      throws SuspectedFileSizeException, InvalidPathException, IOException {
    for (int i = 0; i < sFiles; i ++) {
      String filePath = sFilePath + "/part-" + i;
      TachyonFile file = sTachyonClient.getFile(filePath);
      file.open(OpType.WRITE_CACHE);

      ByteBuffer buf = ByteBuffer.allocate(80);
      buf.order(ByteOrder.nativeOrder());
      for (int k = 0; k < 20; k ++) {
        buf.putInt(k);
      }

      buf.flip();
      LOG.info("Writing data to " + filePath);
      CommonUtils.printByteBuffer(LOG, buf);
      buf.flip();
      file.append(buf);
      file.close();
    }
  }

  public static void readFile()
      throws SuspectedFileSizeException, InvalidPathException, IOException {
    for (int i = 0; i < sFiles; i ++) {
      String filePath = sFilePath + "/part-" + i;
      LOG.info("Reading data from " + filePath);
      TachyonFile file = sTachyonClient.getFile(filePath);
      file.open(OpType.READ_TRY_CACHE);
      ByteBuffer buf = file.readByteBuffer();
      CommonUtils.printByteBuffer(LOG, buf);
      file.close();
    }
  }

  public static void main(String[] args)
      throws SuspectedFileSizeException, InvalidPathException, IOException, 
      FileDoesNotExistException, FileAlreadyExistException, TException {
    if (args.length != 3) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION + 
          "-jar-with-dependencies.jar " +
          "tachyon.examples.BasicCheckpointTest <TachyonMasterHostName> <FilePath> <Files>");
      System.exit(-1);
    }
    sTachyonClient = TachyonClient.getClient(new InetSocketAddress(args[0], Config.MASTER_PORT));
    sFilePath = args[1];
    sFiles = Integer.parseInt(args[2]);
    createDependency();
    writeFile();
    readFile();
  }
}
