package tachyon.client;

import java.io.IOException;
import java.io.OutputStream;

public class FileOutputStream extends OutputStream {
  private final TachyonFile FILE; 

  public FileOutputStream(TachyonFile file) {
    FILE = file;
  }

  @Override
  public void write(int b) throws IOException {
    FILE.append((byte) b);
  }

  @Override
  public void write(byte b[]) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    FILE.append(b, off, len);
  }

  @Override
  public void close() throws IOException {
    FILE.close();
  }
}
