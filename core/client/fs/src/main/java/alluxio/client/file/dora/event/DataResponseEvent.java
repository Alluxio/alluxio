package alluxio.client.file.dora.event;

import alluxio.client.file.dora.PartialReadException;
import java.io.IOException;
import java.nio.ByteBuffer;
import io.netty.buffer.ByteBuf;

public class DataResponseEvent implements ResponseEvent {

  private final ByteBuf mByteBuf;

  public DataResponseEvent(ByteBuf data) {
    mByteBuf = data;
  }

  @Override
  public void postProcess(ResponseEventContext responseEventContext) throws PartialReadException{
    // do nothing
    int readableBytes = mByteBuf.readableBytes();
    int sliceEnd = Math.min(readableBytes,
        responseEventContext.getLengthToRead() - responseEventContext.getBytesRead());
    // todo(bowen): handle case where ByteBuf does not support getting a bytebuffer
    ByteBuffer toWrite = mByteBuf.nioBuffer(0, sliceEnd);
    try {
      int bytesRead = responseEventContext.getRelatedOutChannel().write(toWrite);
      responseEventContext.increaseBytesRead(bytesRead);
    } catch (IOException ioe) {
      throw new PartialReadException(responseEventContext.getLengthToRead(),
          responseEventContext.getBytesRead(), PartialReadException.CauseType.OUTPUT, ioe);
    } finally {
      // previously retained in packet read handler
      mByteBuf.release();
    }
  }

}
