package alluxio.client.file.cache;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Meta data of each file cached.
 */
public class FileInfo {
  private final String mFileId;
  private final AtomicLong mSize;
  private final AtomicInteger mPageCount;
  private String mPath;

  /**
   * Constructor of FileInfo.
   * @param fileId
   * @param path
   * @param initialSize
   */
  public FileInfo(String fileId, String path, long initialSize) {
    mFileId = fileId;
    mPath = path;
    mSize = new AtomicLong(initialSize);
    mPageCount = new AtomicInteger(0);
  }

  /**
   * @return file id
   */
  public String getFileId() {
    return mFileId;
  }

  /**
   * @return path
   */
  public String getPath() {
    return mPath;
  }

  /**
   * Set the path.
   * @param path
   */
  public void setPath(String path) {
    mPath = path;
  }

  /**
   * @return the size of the file
   */
  public long getSize() {
    return mSize.get();
  }

  /**
   * Increase the size of th file.
   * @param delta the value to add
   */
  public void increaseSize(long delta) {
    mSize.addAndGet(delta);
  }

  /**
   * Increase the page count.
   * @return the updated page count
   */
  public int increasePageCount() {
    return mPageCount.incrementAndGet();
  }

  /**
   * Decrease the page count.
   * @return the updated page count
   */
  public int decreasePageCount() {
    return mPageCount.decrementAndGet();
  }

  /**
   * @return the page count
   */
  public int getPageCount() {
    return mPageCount.get();
  }
}
