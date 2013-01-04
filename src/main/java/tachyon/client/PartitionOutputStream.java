package tachyon.client;

import java.io.IOException;
import java.io.OutputStream;

import tachyon.CommonUtils;
import tachyon.thrift.OutOfMemoryForPinDatasetException;

public class PartitionOutputStream extends OutputStream {
  private final Partition PARTITION; 

  public PartitionOutputStream(Partition partition) {
    PARTITION = partition;
  }

  @Override
  public void write(int b) throws IOException {
    PARTITION.append(b);
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

    try {
      PARTITION.append(b, off, len);
    } catch (OutOfMemoryForPinDatasetException e) {
      CommonUtils.runtimeException(e);
    }
  }

  @Override
  public void close() {
    PARTITION.close();
  }
}
