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
public class AlluxioProfilerClient implements ProfilerClient {
  private static final int CHUNK_SIZE = 10 * Constants.MB;
  private static final byte[] DATA = new byte[CHUNK_SIZE];

  static {
    Arrays.fill(DATA, (byte)0xCF);
  }

  private final FileSystem mClient;

  public AlluxioProfilerClient() {
    mClient = FileSystem.Factory.get();
  }

  @Override
  public void cleanup(String dir) throws IOException {
    try {
      AlluxioURI path = new AlluxioURI(dir);
      try {
        mClient.delete(path,
            DeletePOptions.newBuilder().setRecursive(true).setAlluxioOnly(false).build());
      } catch (FileDoesNotExistException e) {
        // ok if it doesn't exist already
      }

      mClient.createDirectory(path,
          CreateDirectoryPOptions.newBuilder().setRecursive(true).setAllowExists(true).build());
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
        mClient.createDirectory(subUri,
            CreateDirectoryPOptions.newBuilder().setRecursive(true).setAllowExists(true).build());
        for (int j = 0; j < filesPerDir; j++) {
          AlluxioURI filePath = new AlluxioURI(PathUtils.concatPath(subUri.getPath(), j));
          try (FileOutStream stream = mClient.createFile(filePath,
              CreateFilePOptions.newBuilder().build())) {
            for (int k = 0; k < fileSize / CHUNK_SIZE; k++) {
              stream.write(DATA);
            }
            stream.write(DATA, 0, (int)(fileSize % CHUNK_SIZE)); // Write any remaining data
          }
          createdFiles++;
        }
      } catch (AlluxioException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
