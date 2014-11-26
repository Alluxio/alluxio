package tachyon.client.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.client.RemoteBlockReader;
import tachyon.worker.DataServerMessage;

public class TCPRemoteBlockReader implements RemoteBlockReader {

  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  @Override
  public ByteBuffer readRemoteBlock(String host, int port, long blockId, long offset, long length)
      throws IOException {
    InetSocketAddress address = new InetSocketAddress(host, port);
    SocketChannel socketChannel = SocketChannel.open();
    try {
      socketChannel.connect(address);

      LOG.info("Connected to remote machine " + address);
      DataServerMessage sendMsg =
          DataServerMessage.createBlockRequestMessage(blockId, offset, length);
      while (!sendMsg.finishSending()) {
        sendMsg.send(socketChannel);
      }

      LOG.info("Data " + blockId + " to remote machine " + address + " sent");

      DataServerMessage recvMsg = DataServerMessage.createBlockResponseMessage(false, blockId);
      while (!recvMsg.isMessageReady()) {
        int numRead = recvMsg.recv(socketChannel);
        if (numRead == -1) {
          break;
        }
      }
      LOG.info("Data " + blockId + " from remote machine " + address + " received");

      if (!recvMsg.isMessageReady()) {
        LOG.info("Data " + blockId + " from remote machine is not ready.");
        return null;
      }

      if (recvMsg.getBlockId() < 0) {
        LOG.info("Data " + recvMsg.getBlockId() + " is not in remote machine.");
        return null;
      }

      return recvMsg.getReadOnlyData();
    } finally {
      socketChannel.close();
    }
  }
}
