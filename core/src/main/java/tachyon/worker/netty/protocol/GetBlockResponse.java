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

public final class GetBlockResponse implements NettyWritable {
  private static final int MESSAGE_LENGTH = 3 * Longs.BYTES;

  private final long mBlockId;
  private final long mOffset;
  private final long mLength;
  private final FileChannel mData;

  public GetBlockResponse(long blockId, long offset, long length, FileChannel data) {
    mBlockId = blockId;
    mOffset = offset;
    mLength = length;
    mData = data;
  }

  public long getBlockId() {
    return mBlockId;
  }

  public long getLength() {
    return mLength;
  }

  public FileChannel getData() {
    return mData;
  }

  public long getOffset() {
    return mOffset;
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
