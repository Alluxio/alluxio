package alluxio.master.catalog;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

/**
 * {@link OutputFile} implementation using the Alluxio API.
 */
public class AlluxioOutputFile implements OutputFile {
  public static OutputFile fromPath(FileSystem fs, String path) {
    return new AlluxioOutputFile(new AlluxioURI(path), fs);
  }

  private final AlluxioURI mPath;
  private final FileSystem mFileSystem;

  private AlluxioOutputFile(AlluxioURI path, FileSystem fs) {
    mPath = path;
    mFileSystem = fs;
  }

  @Override
  public PositionOutputStream create() {
    try {
      return AlluxioStreams.wrap(mFileSystem.createFile(mPath));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create file:" + mPath, e);
    }
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    try {
      mFileSystem.delete(mPath);
      return AlluxioStreams.wrap(mFileSystem.createFile(mPath));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create file:" + mPath, e);
    }
  }

  public AlluxioURI getPath() {
    return mPath;
  }

  @Override
  public String location() {
    return mPath.toString();
  }

  @Override
  public InputFile toInputFile() {
    return AlluxioInputFile.fromPath(mFileSystem, mPath.toString());
  }

  @Override
  public String toString() {
    return location();
  }
}
