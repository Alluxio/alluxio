package tachyon.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import tachyon.thrift.InvalidPathException;

public class TestReadBig {
  public static void main(String[] args) {
    String localFileName = "/mnt/ramdisk/tachyonworker/0";
    try {
      RandomAccessFile mFile = new RandomAccessFile(localFileName, "r");
      int mSizeBytes = (int) mFile.length();
      System.out.println("Size = " + mSizeBytes);
      FileChannel mInChannel = mFile.getChannel();
      MappedByteBuffer ret = mInChannel.map(FileChannel.MapMode.READ_ONLY, 0, mSizeBytes);
      ret.order(ByteOrder.nativeOrder());
      int cnt = 0;
      while (true) {
        System.out.println(cnt + " " + ret.getChar(cnt));
        Thread.sleep(100);
        cnt += 1000000;
      }
    } catch (FileNotFoundException e) {
      System.out.println(localFileName + " is not on local disk. " + e.getMessage());
    } catch (IOException e) {
      System.out.println(localFileName + " is not on local disk. " + e.getMessage());
    } catch (InterruptedException e) {
      System.out.println(localFileName + " is not on local disk. " + e.getMessage());
    }
  }
}
