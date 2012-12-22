package tachyon.client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import tachyon.CommonUtils;

/**
 * The current implementation is not efficient for all cases.
 * 
 * TODO: Let the Partition to learn from JAVA I/O.
 * 
 * @author Haoyuan
 */
public class PartitionInputStream extends InputStream {
  private ByteBuffer mData;

  public PartitionInputStream(Partition partition) {
    try {
      mData = partition.readByteBuffer();
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  @Override
  public int read() throws IOException {
    if (mData.position() < mData.limit()) {
      return mData.get();
    }
    return -1;
  }
}
