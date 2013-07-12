package tachyon.hadoop;

import java.io.IOException;
import java.util.zip.Checksum;

import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.Syncable;

public class TFileOutputStreamHdfs extends FSOutputSummer implements Syncable {

  protected TFileOutputStreamHdfs(Checksum sum, int maxChunkSize,
      int checksumSize) {
    super(sum, maxChunkSize, checksumSize);
    // TODO Auto-generated constructor stub
  }

  @Override
  public void sync() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  /**
   * write the data chunk in <code>b</code> staring at <code>offset</code> with
   * a length of <code>len</code>, and its checksum
   */
  protected void writeChunk(byte[] b, int offset, int len, byte[] checksum)
      throws IOException {
    // TODO Auto-generated method stub
  }
}
