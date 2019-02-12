package alluxio.cli.profiler;

import alluxio.util.io.PathUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HadoopProfilerClient extends ProfilerClient {

  private final FileSystem mClient;

  public HadoopProfilerClient(String hadoopUri) {
    try {
      mClient = FileSystem.get(new URI(hadoopUri), new Configuration());
    } catch (URISyntaxException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void cleanup(String dir) throws IOException {
    if (!sDryRun) {
      mClient.delete(new Path(dir), true);
    } else {
      System.out.println("delete: " + dir);
    }

    if (!sDryRun) {
      mClient.mkdirs(new Path(dir));
    } else {
      System.out.println("create: " + dir);
    }
  }

  @Override
  public void createFiles(String dir, long numFiles, long filesPerDir, long fileSize)
      throws IOException {
    int createdFiles = 0;
    while (createdFiles < numFiles) {
      String subDir = PathUtils.concatPath(dir, createdFiles);
      if (!sDryRun) {
        mClient.create(new Path(subDir));
      } else {
        System.out.println("create: " + subDir);
      }
      for (int j = 0; j < filesPerDir; j++) {
        Path filePath = new Path(PathUtils.concatPath(subDir, j));
        if (!sDryRun) {
          try (FSDataOutputStream stream = mClient.create(filePath)) {
            writeOutput(stream, fileSize);
          }
        } else {
          System.out.println("create: " + filePath);
        }
        createdFiles++;
      }
    }
  }
}
