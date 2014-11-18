package tachyon.worker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import tachyon.thrift.ClientBlockInfo;
import tachyon.worker.DataServerMessage;
import tachyon.worker.DataServerTest.DataServerRequest;

public class TCPDataServerRequest implements DataServerRequest {

  /**
   * Create a new socket to the data port and send a block request. The returned value is the
   * response from the server.
   */
  public DataServerMessage request(final ClientBlockInfo block, final long offset,
      final long length) throws IOException {
    DataServerMessage sendMsg =
        DataServerMessage.createBlockRequestMessage(block.blockId, offset, length);
    SocketChannel socketChannel =
        SocketChannel.open(new InetSocketAddress(block.getLocations().get(0).mHost, block
            .getLocations().get(0).mSecondaryPort));
    try {
      while (!sendMsg.finishSending()) {
        sendMsg.send(socketChannel);
      }
      DataServerMessage recvMsg =
          DataServerMessage.createBlockResponseMessage(false, block.blockId, offset, length);
      while (!recvMsg.isMessageReady()) {
        int numRead = recvMsg.recv(socketChannel);
        if (numRead == -1) {
          break;
        }
      }
      return recvMsg;
    } finally {
      socketChannel.close();
    }
  }
}
