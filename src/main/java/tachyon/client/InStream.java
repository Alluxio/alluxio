package tachyon.client;

import java.io.IOException;
import java.io.InputStream;

import tachyon.conf.UserConf;

/**
 * <code>InStream</code> is the base input stream class for TachyonFile streaming input methods.
 * It can only be gotten by calling the methods in <code>tachyon.client.TachyonFile</code>, but
 * can not be initialized by the client code.
 */
public abstract class InStream extends InputStream {
  protected final UserConf USER_CONF = UserConf.get();
  protected final TachyonFile FILE;
  protected final TachyonFS TFS;
  protected final ReadType READ_TYPE;

  InStream(TachyonFile file, ReadType readType) {
    FILE = file;
    TFS = FILE.TFS;
    READ_TYPE = readType;
  }

  @Override
  public abstract int read() throws IOException;

  @Override
  public abstract int read(byte b[]) throws IOException;

  @Override
  public abstract int read(byte b[], int off, int len) throws IOException;

  @Override
  public abstract void close() throws IOException;

  @Override
  public abstract long skip(long n) throws IOException;
}