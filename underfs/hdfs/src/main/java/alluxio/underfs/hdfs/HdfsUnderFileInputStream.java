package alluxio.underfs.hdfs;

import alluxio.underfs.SeekableUnderFileInputStream;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

/**
 * The input stream of HDFS as under filesystem. This input stream supports seeking and can be
 * cached for reuse.
 */
public class HdfsUnderFileInputStream extends SeekableUnderFileInputStream {

  HdfsUnderFileInputStream(FSDataInputStream in) {
    super(in);
  }

  @Override
  public void seek(long position) throws IOException {
    ((FSDataInputStream) mInputStream).seek(position);
  }

  @Override
  public long getPos() {
    try {
      return ((FSDataInputStream) mInputStream).getPos();
    } catch (IOException e) {
      throw new RuntimeException("Failed to get position " + e);
    }
  }
}
