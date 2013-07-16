package tachyon.hadoop;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.Syncable;

import tachyon.client.TachyonFile;
import tachyon.client.WriteType;

public class HdfsFileOutputStream extends OutputStream implements Syncable {

  HdfsFileOutputStream(TachyonFile file, WriteType writeType) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public void sync() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void write(int b) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void write(byte[] b) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void flush() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    
  }
}
