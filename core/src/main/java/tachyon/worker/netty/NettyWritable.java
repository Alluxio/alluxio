package tachyon.worker.netty;

import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.util.List;

public interface NettyWritable {
  List<Object> write(ByteBufAllocator allocator) throws IOException;
}
