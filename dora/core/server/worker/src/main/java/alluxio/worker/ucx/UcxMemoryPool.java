package alluxio.worker.ucx;

import com.google.common.base.Preconditions;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpMemMapParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucs.UcsConstants;

import java.nio.ByteBuffer;

public class UcxMemoryPool {

  // Unpooled allocating
  public static UcpMemory allocateMemory(long length, int memType) {
    if (memType == UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST) {
      ByteBuffer directBuf = ByteBuffer.allocateDirect((int) length);
      UcpMemory allocMem = registerMemory(UcxUtils.getAddress(directBuf), length);
      return allocMem;
    } else if (memType == UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA) {
      // Untested because lack of supported fabric hardwards (GPU with NVLink)
      Preconditions.checkArgument(
          UcsConstants.MEMORY_TYPE.isMemTypeSupported(UcpServer.sGlobalContext.getMemoryTypesMask(),
              UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA),
          "CUDA mem not supported");
      UcpMemMapParams memMapParams = new UcpMemMapParams().allocate().setLength(length)
          .setMemoryType(memType);
      return UcpServer.sGlobalContext.memoryMap(memMapParams);
    } else {
      throw new IllegalStateException("Unrecognized memType.");
    }
  }

  public static UcpMemory registerMemory(long addr, long length) {
    UcpMemory registerdMem = UcpServer.sGlobalContext.memoryMap(new UcpMemMapParams()
        .setAddress(addr).setLength(length).nonBlocking());
    return registerdMem;
  }

}
