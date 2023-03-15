package alluxio.worker.netty;

import alluxio.network.netty.FileTransferType;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.worker.dora.DoraWorker;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.ExecutorService;

public class PositionReadHandler extends ChannelInboundHandlerAdapter {
  private final ExecutorService mExecutor;
  private final DoraWorker mWorker;

  /**
   * The transfer type used by the data server.
   */
  private final FileTransferType mTransferType;
  
  public PositionReadHandler(ExecutorService executor,
      DoraWorker worker, FileTransferType fileTransferType) {
    mExecutor = executor;
    mWorker = worker;
    mTransferType = fileTransferType;
  }
  
  // TODO(lu) implement thread safe worker page file reader
  // TODO(lu) initialize the worker page file reader in channel initializer?

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object object) throws Exception {
    Protocol.ReadRequest msg = ((RPCProtoMessage) object).getMessage().asReadRequest();
    // TODO(lu) read data from worker

    // TODO(lu) write Response.RequestId so that client can know which exact read request it belong to
    RPCProtoMessage response = RPCProtoMessage.createOkResponse(buffer);
    ctx.channel().writeAndFlush(response);
  }
}
