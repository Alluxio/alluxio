package tachyon.client;

import java.io.IOException;
import java.io.InputStream;

/**
 * The current implementation is not efficient for all cases.
 * 
 * TODO: Let the Partition to learn from JAVA I/O.
 * 
 * @author Haoyuan
 */
public class PartitionInputStream extends InputStream {
  private final Partition PARTITION; 

  public PartitionInputStream(Partition partition) {
    PARTITION = partition;
  }

  @Override
  public int read() throws IOException {
    return PARTITION.read();
  }
}
