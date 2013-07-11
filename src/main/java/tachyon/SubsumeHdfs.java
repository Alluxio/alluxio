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

  public static void subsume(TachyonFS tfs, String hdfsAddress, String rootPath, 
      PrefixList excludePathPrefix) throws IOException {
    LOG.info(tfs + " " + hdfsAddress + " " + rootPath + " " + excludePathPrefix);

    Configuration tConf = new Configuration();
    tConf.set("fs.default.name", hdfsAddress + rootPath);
    FileSystem fs = FileSystem.get(tConf);

    Queue<String> pathQueue = new LinkedList<String>();
    if (excludePathPrefix.outList(rootPath)) {
      pathQueue.add(hdfsAddress + rootPath);
    }
    while (!pathQueue.isEmpty()) {
      String path = pathQueue.poll();
      if (fs.isFile(new Path(path))) {
        String filePath =  path.substring(hdfsAddress.length());
        if (tfs.exist(filePath)) {
          LOG.info("File " + filePath + " already exists in Tachyon.");
          continue;
        }
        int fileId = tfs.createFile(filePath, path);
        if (fileId == -1) {
          LOG.info("Failed to create tachyon file: " + filePath);
        } else {
          LOG.info("Create tachyon file " + filePath + " with file id " + fileId + " and "
              + "checkpoint location " + path);
        }
      } else {
        FileStatus[] files = fs.listStatus(new Path(path));
        for (FileStatus status : files) {
          LOG.info("Get: " + status.getPath());
          String filePath = status.getPath().toString().substring(hdfsAddress.length());
          if (excludePathPrefix.outList(filePath)) {
            pathQueue.add(status.getPath().toString());
          }
        }
        String filePath = path.substring(hdfsAddress.length());
        if (!tfs.exist(filePath)) {
          tfs.mkdir(filePath);
        }
      }
    }
  }

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

    PrefixList tExcludePathPrefix = null;
    if (args.length == 4) {
      tExcludePathPrefix = new PrefixList(args[3], ";");
    } else {
      tExcludePathPrefix = new PrefixList(null);
    }

    subsume(TachyonFS.get(args[0]), args[1], args[2], tExcludePathPrefix);
  }
}