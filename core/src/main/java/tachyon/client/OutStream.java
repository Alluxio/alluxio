package tachyon.client;

import java.io.IOException;
import java.io.OutputStream;

import tachyon.conf.UserConf;

/**
 * <code>OutStream</code> is the base output stream class for TachyonFile streaming output methods.
 * It can only be gotten by calling the methods in <code>tachyon.client.TachyonFile</code>, but
 * can not be initialized by the client code.
 */
public abstract class OutStream extends OutputStream {
  protected final UserConf mUserConf = UserConf.get();
  protected final TachyonFile mFile;
  protected final TachyonFS mTachyonFS;
  protected final WriteType mWriteType;

  /**
   * @param file
   *          the output file of the OutStream
   * @param writeType
   *          the OutStream's write type
   */
  OutStream(TachyonFile file, WriteType writeType) {
    mFile = file;
    mTachyonFS = mFile.mTachyonFS;
    mWriteType = writeType;
  }

  /**
   * Cancel the write operations to the OutStream
   * 
   * @throws IOException
   */
  public abstract void cancel() throws IOException;

  @Override
  public abstract void close() throws IOException;

  @Override
  public abstract void flush() throws IOException;

  @Override
  public abstract void write(byte b[]) throws IOException;

  @Override
  public abstract void write(byte b[], int off, int len) throws IOException;

  @Override
  public abstract void write(int b) throws IOException;
}