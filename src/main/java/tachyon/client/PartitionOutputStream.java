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

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
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
