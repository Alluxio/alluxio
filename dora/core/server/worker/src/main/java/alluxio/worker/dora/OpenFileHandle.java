package alluxio.worker.dora;

import alluxio.grpc.FileInfo;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

/**
 * Open File Handle in Dora for write request.
 */
public class OpenFileHandle {
  private final String   mPath;
  private final FileInfo mInfo;
  private final UUID     mUUID;
  private Long           mPos;
  private Long           mLastAccessTimeMs;
  private OutputStream   mOutStream; //outstream from UFS

  private Boolean        mClosed;

  /**
   * Construct a new open file handle.
   * @param path
   * @param info
   * @param outStream
   */
  public OpenFileHandle(String path, FileInfo info, OutputStream outStream) {
    mPath = path;
    mInfo = info;
    mUUID = UUID.randomUUID();
    mOutStream = outStream;
    mPos = 0L;
    mLastAccessTimeMs = System.currentTimeMillis();
    mClosed = Boolean.FALSE;
  }

  /**
   * Get UUID.
   * @return the UUID of this handle
   */
  public UUID getUUID() {
    return mUUID;
  }

  /**
   * Get last accessed time.
   * @return the last accessed time of this handle
   */
  public Long getLastAccessTimeMs() {
    return mLastAccessTimeMs;
  }

  /**
   * Close this handle.
   */
  public void close() {
    mClosed = Boolean.TRUE;
    if (mOutStream != null) {
      try {
        mOutStream.close();
      } catch (IOException e) {
        //;
      }
    }
  }
}
