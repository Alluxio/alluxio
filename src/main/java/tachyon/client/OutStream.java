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
  protected final UserConf USER_CONF = UserConf.get();
  protected final TachyonFile FILE;
  protected final TachyonFS TFS;
  protected final WriteType WRITE_TYPE;

  OutStream(TachyonFile file, WriteType writeType) {
    FILE = file;
    TFS = FILE.TFS;
    WRITE_TYPE = writeType;
  }

  @Override
  public abstract void write(int b) throws IOException;

  @Override
  public abstract void write(byte b[]) throws IOException;

  @Override
  public abstract void write(byte b[], int off, int len) throws IOException;

  @Override
  public abstract void flush() throws IOException;

  @Override
  public abstract void close() throws IOException;

  public abstract void cancel() throws IOException;
}