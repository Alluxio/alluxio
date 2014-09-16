package tachyon.worker.netty.protocol;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultFileRegion;

import tachyon.conf.WorkerConf;
import tachyon.worker.netty.NettyWritable;
import tachyon.worker.nio.DataServerMessage;

public final class GetBlockResponse implements NettyWritable {
  private static final int MESSAGE_LENGTH = 3 * Longs.BYTES;

  private final long blockId;
  private final long offset;
  private final long length;
  private final FileChannel data;

  public GetBlockResponse(long blockId, long offset, long length, FileChannel data) {
    this.blockId = blockId;
    this.offset = offset;
    this.length = length;
    this.data = data;
  }

  public long getBlockId() {
    return blockId;
  }

  public long getLength() {
    return length;
  }

  public FileChannel getData() {
    return data;
  }

  public long getOffset() {
    return offset;
  }

  @Override
  public List<Object> write(ByteBufAllocator allocator) throws IOException {
    List<Object> rsp = Lists.newArrayListWithCapacity(2);
    rsp.add(createHeader(allocator));
    switch (WorkerConf.get().NETTY_FILE_TRANSFER_TYPE) {
      case MAPPED:
        MappedByteBuffer data =
            getData().map(FileChannel.MapMode.READ_ONLY, getOffset(), getLength());
        rsp.add(Unpooled.wrappedBuffer(data));
        break;
      case TRANSFER:
        rsp.add(new DefaultFileRegion(getData(), getOffset(), getLength()));
        break;
      default:
        throw new AssertionError("Unknown file transfer type: "
            + WorkerConf.get().NETTY_FILE_TRANSFER_TYPE);
    }
    return rsp;
  }

  private ByteBuf createHeader(ByteBufAllocator allocator) {
    ByteBuf header = allocator.buffer(MESSAGE_LENGTH);
    header.writeLong(getBlockId());
    header.writeLong(getOffset());
    header.writeLong(getLength());
    return header;
  }
}
