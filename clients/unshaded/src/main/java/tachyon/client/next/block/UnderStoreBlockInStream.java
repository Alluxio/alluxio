package tachyon.client.next.block;

import tachyon.client.next.ClientContext;
import tachyon.underfs.UnderFileSystem;

import java.io.IOException;
import java.io.InputStream;

public class UnderStoreBlockInStream extends BlockInStream {
  private final String mUfsPath;

  private long mPos;
  private InputStream mUnderStoreStream;

  public UnderStoreBlockInStream(String ufsPath) throws IOException {
    mUfsPath = ufsPath;
    resetUnderStoreStream();
  }

  @Override
  public int read() throws IOException {
    int data = mUnderStoreStream.read();
    mPos ++;
    return data;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < mPos) {
      resetUnderStoreStream();
      if (skip(pos) != pos) {
        throw new IOException("Failed to seek backward to " + pos);
      }
    } else {
      if (skip(mPos - pos) != mPos - pos) {
        throw new IOException("Failed to seek forward to " + pos);
      }
    }
  }

  @Override
  public int read(byte[] b) throws IOException {
    int data = mUnderStoreStream.read(b);
    mPos ++;
    return data;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int bytesRead = mUnderStoreStream.read(b, off, len);
    mPos += bytesRead;
    return bytesRead;
  }

  @Override
  public long skip(long n) throws IOException {
    long skipped = mUnderStoreStream.skip(n);
    mPos += skipped;
    return skipped;
  }

  private void resetUnderStoreStream() throws IOException {
    if (null != mUnderStoreStream) {
      mUnderStoreStream.close();
    }
    UnderFileSystem ufs = UnderFileSystem.get(mUfsPath, ClientContext.getConf());
    mUnderStoreStream = ufs.open(mUfsPath);
    mPos = 0;
  }
}
