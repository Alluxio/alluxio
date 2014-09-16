package tachyon.client;

import java.io.IOException;

/**
 * <code> EmptyBlockInStream </code> is a <code> InStream </code> can not read anything.
 */
public class EmptyBlockInStream extends InStream {
  /**
   * @param file the file the block belongs to
   * @param readType the InStream's read type
   */
  EmptyBlockInStream(TachyonFile file, ReadType readType) {
    super(file, readType);
  }

  @Override
  public void close() throws IOException {}

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
  public void seek(long pos) throws IOException {
    if (pos < 0) {
      throw new IOException("pos is negative: " + pos);
    }
  }

  @Override
  public long skip(long n) throws IOException {
    return 0;
  }
}
