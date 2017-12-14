package alluxio.underfs;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;

public abstract class SeekableUnderFileInputStream extends InputStream {

  protected final InputStream mInputStream;
  private Long mResourceId;
  private String mFilePath;

  public SeekableUnderFileInputStream(InputStream inputStream) {
    Preconditions.checkNotNull(inputStream, "inputStream");
    mInputStream = inputStream;
  }

  public void setResourceId(long resourceId) {
    Preconditions.checkArgument(resourceId >= 0, "resource id should be positive");
    mResourceId = resourceId;
  }

  public long getResourceId() {
    return mResourceId;
  }

  public void setFilePath(String filePath) {
    mFilePath = filePath;
  }

  public String getFilePath() {
    return mFilePath;
  }

  @Override
  public void close() throws IOException {
    mInputStream.close();
  }

  @Override
  public int read() throws IOException {
    return mInputStream.read();
  }

  @Override
  public int read(byte b[]) throws IOException {
    return mInputStream.read(b);
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    return mInputStream.read(b, off, len);
  }

  @Override
  public long skip(long n) throws IOException {
    return mInputStream.skip(n);
  }

  @Override
  public int available() throws IOException {
    return mInputStream.available();
  }

  @Override
  public synchronized void mark(int readlimit) {
    mInputStream.mark(readlimit);
  }

  @Override
  public synchronized void reset() throws IOException {
    mInputStream.reset();
  }

  @Override
  public boolean markSupported() {
    return mInputStream.markSupported();
  }

  public abstract void seek(long position) throws IOException;
}
