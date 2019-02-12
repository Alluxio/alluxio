package alluxio.cli.profiler;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.util.io.PathUtils;

import java.io.IOException;
import java.util.Arrays;

/**
 * Profiler client for Alluxio.
 */
public class AlluxioProfilerClient extends ProfilerClient {


  private final FileSystem mClient;

  public AlluxioProfilerClient() {
    mClient = FileSystem.Factory.get();
  }

  @Override
  public void cleanup(String dir) throws IOException {
    try {
      AlluxioURI path = new AlluxioURI(dir);
      try {
        if (!sDryRun) {
          mClient.delete(path,
              DeletePOptions.newBuilder().setRecursive(true).setAlluxioOnly(false).build());
        } else {
          System.out.println("Delete: " + path);
        }
      } catch (FileDoesNotExistException e) {
        // ok if it doesn't exist already
      }

      if (!sDryRun) {
        mClient.createDirectory(path,
            CreateDirectoryPOptions.newBuilder().setRecursive(true).setAllowExists(true).build());
      } else {
        System.out.println("create directory: " + path);
      }
    } catch (AlluxioException e) {
      throw new IOException(e);
    }

    return;
  }

  @Override
  public void createFiles(String dir, long numFiles, long filesPerDir, long fileSize) throws IOException {
    int createdFiles = 0;
    while (createdFiles < numFiles) {
      try {
        String subDir = PathUtils.concatPath(dir, createdFiles);
        AlluxioURI subUri = new AlluxioURI(subDir);
        if (!sDryRun) {
          mClient.createDirectory(subUri,
              CreateDirectoryPOptions.newBuilder().setRecursive(true).setAllowExists(true).build());
        } else {
          System.out.println("Create directory: " + subUri);
        }
        for (int j = 0; j < filesPerDir; j++) {
          AlluxioURI filePath = new AlluxioURI(PathUtils.concatPath(subUri.getPath(), j));
          if (!sDryRun) {
            try (FileOutStream stream = mClient.createFile(filePath,
                CreateFilePOptions.newBuilder().build())) {
              writeOutput(stream, fileSize);
            }
          } else {
            System.out.println("Create file: " + filePath);
          }
          createdFiles++;
        }
      } catch (AlluxioException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
