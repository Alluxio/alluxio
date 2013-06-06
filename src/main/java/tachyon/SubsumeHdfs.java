package tachyon;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import tachyon.client.TachyonFS;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.SuspectedFileSizeException;

public class SubsumeHdfs {
  private static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private static TachyonFS sTachyonClient;
  private static String sFilePath = null;
  private static String sHdfsAddress = null;
  private static PrefixList sExcludePathPrefix = null;

  public static void main(String[] args)
      throws SuspectedFileSizeException, InvalidPathException, IOException,
      FileDoesNotExistException, FileAlreadyExistException, TException {
    if (!(args.length == 3 || args.length == 4)) {
      String prefix = "java -cp target/tachyon-" + Version.VERSION + "-jar-with-dependencies.jar " + 
          "tachyon.SubsumeHdfs ";
      System.out.println("Usage: " + prefix + "<TachyonAddress> <HdfsAddress> <Path> " +
          "[<ExcludePathPrefix, separated by ;>]");
      System.out.println("Example: " + prefix + 
          "127.0.0.1:19998 hdfs://localhost:54310 / /tachyon");
      System.exit(-1);
    }
    sTachyonClient = TachyonFS.get(args[0]);
    sHdfsAddress = args[1];
    sFilePath = args[2];
    if (args.length == 4) {
      sExcludePathPrefix = new PrefixList(args[3], ";");
    } else {
      sExcludePathPrefix = new PrefixList(null);
    }

    Configuration tConf = new Configuration();
    tConf.set("fs.default.name", sHdfsAddress + sFilePath);
    FileSystem fs = FileSystem.get(tConf);

    Queue<String> pathQueue = new LinkedList<String>();
    if (sExcludePathPrefix.outList(sFilePath)) {
      pathQueue.add(sHdfsAddress + sFilePath);
    }
    while (!pathQueue.isEmpty()) {
      String path = pathQueue.poll();
      if (fs.isFile(new Path(path))) {
        String filePath =  path.substring(sHdfsAddress.length());
        if (sTachyonClient.exist(filePath)) {
          LOG.info("File " + filePath + " already exists in Tachyon.");
          continue;
        }
        int fileId = sTachyonClient.createFile(filePath);
        if (fileId == -1) {
          LOG.info("Failed to create tachyon file: " + filePath);
        } else {
          sTachyonClient.addCheckpointPath(fileId, path);
          LOG.info("Create tachyon file " + filePath + " with file id " + fileId + " and "
              + "checkpoint location " + path);
        }
      } else {
        FileStatus[] files = fs.listStatus(new Path(path));
        for (FileStatus status : files) {
          LOG.info("Get: " + status.getPath());
          String filePath = status.getPath().toString().substring(sHdfsAddress.length());
          if (sExcludePathPrefix.outList(filePath)) {
            pathQueue.add(status.getPath().toString());
          }
        }
        String filePath = path.substring(sHdfsAddress.length());
        try {
          if (!sTachyonClient.exist(filePath)) {
            sTachyonClient.mkdir(filePath);
          }
        } catch (FileAlreadyExistException e) {
        }
      }
    }
  }
}