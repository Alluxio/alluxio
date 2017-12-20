package alluxio.worker.block;

import alluxio.underfs.SeekableUnderFileInputStream;

import com.google.common.base.Preconditions;

import java.io.IOException;

public class CachedSeekableInputStream extends SeekableUnderFileInputStream {
  /** A unique resource id annotated for resource tracking. */
  private Long mResourceId;
  /** The file path of the input stream. */
  private String mFilePath;
  /**
   * Creates a new {@link CachedSeekableInputStream}.
   *
   * @param inputStream the input stream from the under storage
   */
  public CachedSeekableInputStream(SeekableUnderFileInputStream inputStream, long resourceId, String filePath) {
    super(inputStream);
    Preconditions.checkArgument(resourceId >= 0, "resource id should be positive");
    mResourceId = resourceId;
    mFilePath = filePath;
  }

  /**
   * @return the resource id
   */
  public long getResourceId() {
    return mResourceId;
  }

  /**
   * @return the under file path
   */
  public String getFilePath() {
    return mFilePath;
  }

  @Override
  public void seek(long pos) throws IOException {
    ((SeekableUnderFileInputStream) in).seek(pos);
  }

  @Override
  public long getPos() {
    return ((SeekableUnderFileInputStream) in).getPos();
  }
}
