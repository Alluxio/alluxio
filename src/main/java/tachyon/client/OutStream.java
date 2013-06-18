package tachyon.client;

import java.io.IOException;
import java.io.OutputStream;

/**
 * <code>OutStream</code> is the base output stream class for TachyonFile streaming output methods.
 * It can only be gotten by calling the methods in <code>tachyon.client.TachyonFile</code>, but
 * can not be initialized by the client code.
 */
public abstract class OutStream extends OutputStream {
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
}