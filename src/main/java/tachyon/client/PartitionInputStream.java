package tachyon.client;

import java.io.IOException;
import java.io.InputStream;

public class PartitionInputStream extends InputStream {
  private final Partition PARTITION; 

  public PartitionInputStream(Partition partition) {
    PARTITION = partition;
  }

  @Override
  public int read() throws IOException {
    return PARTITION.read();
  }

  @Override
  public int read(byte b[]) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    return PARTITION.read(b, off, len);
  }
}
