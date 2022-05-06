package alluxio.fuse.file;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;

public final class FuseFileEntry<T extends FuseFileStream>
    implements Closeable {
  private final long mId;
  private final T mFileStream;
  // Path is likely to be changed when fuse rename() is called
  private String mPath;

  /**
   * Constructs a new {@link FuseFileEntry} for an Alluxio file.
   *
   * @param id the id of the file
   * @param path the path of the file
   * @param fileStream the in/out stream of the file
   */
  public FuseFileEntry(long id, String path, T fileStream) {
    Preconditions.checkArgument(id != -1 && !path.isEmpty());
    Preconditions.checkArgument(fileStream != null);
    mId = id;
    mFileStream = fileStream;
    mPath = path;
  }

  /**
   * @return the id of the file
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the path of the file
   */
  public String getPath() {
    return mPath;
  }

  /**
   * Gets the opened output stream for this open file entry. The value returned can be {@code null}
   * if the file is not open for writing.
   *
   * @return an opened input stream for the open alluxio file, or null
   */
  public T getFileStream() {
    return mFileStream;
  }

  /**
   * Closes the underlying open streams.
   */
  @Override
  public void close() throws IOException {
    if (mFileStream != null) {
      mFileStream.close();
    }
  }
}
