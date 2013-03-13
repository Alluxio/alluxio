package tachyon.client;

import java.io.IOException;
import java.io.InputStream;

public class TFileInputStream extends InputStream {
  private final TachyonFile FILE; 

  public TFileInputStream(TachyonFile file) {
    FILE = file;
  }

  @Override
  public int read() throws IOException {
    return FILE.read();
  }

  @Override
  public int read(byte b[]) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    return FILE.read(b, off, len);
  }
  
  @Override
  public void close() throws IOException {
    FILE.close();
  }
}
