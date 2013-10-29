package tachyon.client;

import java.io.IOException;

/**
 * <code> EmptyBlockInStream </code> is a <code> InStream </code> can not read anything.
 */
public class EmptyBlockInStream extends InStream {
  EmptyBlockInStream(TachyonFile file, ReadType readType) {
    super(file, readType);
  }

  @Override
  public int read() throws IOException {
    return -1;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return b.length == 0 ? 0 : -1;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public long skip(long n) throws IOException {
    return 0;
  }

  @Override
  public void seek(long pos) throws IOException {
    // do nothing
  }
}