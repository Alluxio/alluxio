package alluxio.master.catalog;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;

import alluxio.underfs.UnderFileSystem;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

/**
 * {@link OutputFile} implementation using the Alluxio API.
 */
public class AlluxioOutputFile implements OutputFile {
  public static OutputFile fromPath(UnderFileSystem fs, String path) {
    return new AlluxioOutputFile(path, fs);
  }

  private final String mPath;
  private final UnderFileSystem mFileSystem;

  private AlluxioOutputFile(String path, UnderFileSystem fs) {
    mPath = path;
    mFileSystem = fs;
  }

  @Override
  public PositionOutputStream create() {
    try {
      return AlluxioStreams.wrap(mFileSystem.createNonexistingFile(mPath));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create file:" + mPath, e);
    }
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    try {
      return AlluxioStreams.wrap(mFileSystem.create(mPath));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create file:" + mPath, e);
    }
  }

  public String getPath() {
    return mPath;
  }

  @Override
  public String location() {
    return mPath;
  }

  @Override
  public InputFile toInputFile() {
    return AlluxioInputFile.fromPath(mFileSystem, mPath);
  }

  @Override
  public String toString() {
    return location();
  }
}
