package tachyon.client;

import java.io.IOException;
import java.io.OutputStream;

public class TFileOutputStream extends OutputStream {
  private final TachyonFile FILE; 

  public TFileOutputStream(TachyonFile file) {
    FILE = file;
  }

  @Override
  public void write(int b) throws IOException {
    FILE.append(b);
  }

  public void write(byte b[]) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    FILE.append(b, off, len);
  }

  @Override
  public void close() throws IOException {
    FILE.close();
  }
}
