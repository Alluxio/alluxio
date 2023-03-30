package alluxio.client.file.dora.event;

import java.nio.channels.WritableByteChannel;

/**
 * Response event context is used for recording the following information during
 * the period of processing a response event:
 * <li>the length to read</li>
 * <li>the number of bytes read</li>
 * <li>the related outbound channel</li>
 */
public class ResponseEventContext {

  private final int mLengthToRead;

  private int mBytesRead;

  private final WritableByteChannel mRelatedOutChannel;

  public ResponseEventContext(int lengthToRead, int bytesRead, WritableByteChannel relatedOutChannel) {
    this.mLengthToRead = lengthToRead;
    this.mBytesRead = bytesRead;
    this.mRelatedOutChannel = relatedOutChannel;
  }

  public int getLengthToRead() {
    return mLengthToRead;
  }

  public int getBytesRead() {
    return mBytesRead;
  }

  public WritableByteChannel getRelatedOutChannel() {
    return mRelatedOutChannel;
  }

  public void increaseBytesRead(int bytesRead) {
    mBytesRead += bytesRead;
  }

}
