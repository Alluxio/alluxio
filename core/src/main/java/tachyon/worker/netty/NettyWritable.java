package tachyon.worker.netty;

import java.io.IOException;
import java.util.List;

import io.netty.buffer.ByteBufAllocator;

public interface NettyWritable {
  List<Object> write(ByteBufAllocator allocator) throws IOException;
}
