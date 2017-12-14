package alluxio.underfs.hdfs;

import alluxio.underfs.SeekableUnderFileInputStream;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

public class HdfsUnderFileInputStream extends SeekableUnderFileInputStream {

  HdfsUnderFileInputStream(FSDataInputStream in) {
    super(in);
  }

  @Override
  public void seek(long position) throws IOException {
    ((FSDataInputStream) mInputStream).seek(position);
  }

}
