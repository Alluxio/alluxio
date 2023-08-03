package alluxio.fuse.file;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * foobar.
 */
public class FuseRandomAccessStream implements FuseFileStream {
  private static final int COPY_TO_LOCAL_BUFFER_SIZE_DEFAULT = 64 * Constants.MB;

  private final FileSystem mFileSystem;
  private final RandomAccessFile mRandomAccessFile;
  private final AlluxioURI mURI;

  private final File mTmpFile;

  /**
   * barboo.
   * @param fileSystem
   * @param uri
   * @throws IOException
   * @throws AlluxioException
   */
  public FuseRandomAccessStream(FileSystem fileSystem, AlluxioURI uri) {
    try {
      mFileSystem = fileSystem;
      mTmpFile = File.createTempFile("alluxio-fuse-random-access", null);
      FileInStream is = mFileSystem.openFile(uri);
      FileOutputStream out = new FileOutputStream(mTmpFile);
      byte[] buf = new byte[COPY_TO_LOCAL_BUFFER_SIZE_DEFAULT];
      int t = is.read(buf);
      while (t != -1) {
        out.write(buf, 0, t);
        t = is.read(buf);
      }
      out.close();
      mRandomAccessFile = new RandomAccessFile(mTmpFile, "rw");
      mURI = uri;
      is.close();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public int read(ByteBuffer buf, long size, long offset) {
    try {
      mRandomAccessFile.seek(offset);
      return mRandomAccessFile.read(buf.array(), 0, (int) size);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void write(ByteBuffer buf, long size, long offset) {
    try {
      int sz = (int) size;
      final byte[] dest = new byte[sz];
      buf.get(dest, 0, sz);
      mRandomAccessFile.seek(offset);
      mRandomAccessFile.write(dest, 0, (int) size);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public FileStatus getFileStatus() {
    try {
      return new FileStatus(mRandomAccessFile.length());
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void flush() {
    try {
      // TODO(JiamingMai): rename it to a tmp file before deletion
      mFileSystem.delete(mURI);
      FileOutStream fos = mFileSystem.createFile(mURI);

      long pos = mRandomAccessFile.getFilePointer();
      mRandomAccessFile.seek(0);
      FileChannel channel = mRandomAccessFile.getChannel();
      ByteBuffer buf = ByteBuffer.allocate(COPY_TO_LOCAL_BUFFER_SIZE_DEFAULT);
      while (channel.read(buf) != -1) {
        buf.flip();
        fos.write(buf.array(), 0, buf.limit());
      }
      fos.close();
      mRandomAccessFile.seek(pos);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void truncate(long size) {
    try {
      mRandomAccessFile.setLength(size);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException();
    }
  }

  @Override
  public void close() {
    try {
      // TODO(JiamingMai): rename it to a tmp file before deletion
      mFileSystem.delete(mURI);
      FileOutStream fos = mFileSystem.createFile(mURI);
      mRandomAccessFile.seek(0);
      FileChannel channel = mRandomAccessFile.getChannel();
      ByteBuffer buf = ByteBuffer.allocate(COPY_TO_LOCAL_BUFFER_SIZE_DEFAULT);
      while (channel.read(buf) != -1) {
        buf.flip();
        fos.write(buf.array(), 0, buf.limit());
      }
      mRandomAccessFile.close();
      fos.close();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } finally {
      mTmpFile.delete();
    }
  }
}
