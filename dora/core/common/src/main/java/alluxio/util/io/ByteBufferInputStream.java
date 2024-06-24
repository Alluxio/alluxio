package alluxio.util.io;

import com.google.common.base.Preconditions;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends DataInputStream {
  private final InternalInputStream mInternalInputStream;

  public static class InternalInputStream extends InputStream {
    private final ByteBuffer mBuffer;

    public InternalInputStream(ByteBuffer buffer) {
      Preconditions.checkArgument(buffer.isDirect(), "Input bytebuffer must be direct.");
      mBuffer = buffer.duplicate();
    }

    @Override
    public int read() throws IOException {
      try {
        return mBuffer.get();
      } catch (BufferUnderflowException ex) {
        throw new IOException(ex);
      }
    }

    @Override
    public void close() throws IOException {
      // DO NOTHING
    }
  }

  public static ByteBufferInputStream getInputStream(ByteBuffer buffer) {
    InternalInputStream internalIs = new InternalInputStream(buffer);
    return new ByteBufferInputStream(internalIs);
  }

  public ByteBufferInputStream(InternalInputStream internalIs) {
    super(internalIs);
    mInternalInputStream = internalIs;
  }

  public void read(ByteBuffer targetBuffer, int len) throws IOException {
    if (targetBuffer.remaining() < len) {
      throw new IOException("Not enough space left in targetBuffer to fill " + len + " bytes");
    }
    for (int i = 0; i < len; i++) {
      targetBuffer.put((byte)super.read());
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
  }
}
