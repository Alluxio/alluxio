package tachyon.examples;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import tachyon.Version;
import tachyon.client.TachyonClient;
import tachyon.conf.CommonConf;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.SuspectedFileSizeException;

public class ConsumeHdfs {
  private static Logger LOG = Logger.getLogger(CommonConf.get().LOGGER_TYPE);

  private static TachyonClient sTachyonClient;
  private static String sFilePath = null;
  private static String sHdfsAddress = null;

  public static void main(String[] args)
      throws SuspectedFileSizeException, InvalidPathException, IOException,
      FileDoesNotExistException, TException {
    if (args.length != 3) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION + 
          "-jar-with-dependencies.jar " + 
          "tachyon.examples.ConsumeHdfs <TachyonAddress> <HdfsAddress> <Path>");
      System.exit(-1);
    }
    sTachyonClient = TachyonClient.getClient(args[0]);
    sHdfsAddress = args[1];
    sFilePath = args[2];

    Configuration tConf = new Configuration();
    tConf.set("fs.default.name", sHdfsAddress + sFilePath);
    FileSystem fs = FileSystem.get(tConf);

    Queue<String> pathQueue = new LinkedList<String>();
    pathQueue.add(sHdfsAddress + sFilePath);
    while (!pathQueue.isEmpty()) {
      String path = pathQueue.poll();
      if (fs.isFile(new Path(path))) {
        String filePath =  path.substring(sHdfsAddress.length());
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
          pathQueue.add(status.getPath().toString());
        }
      }
    }
  }
}