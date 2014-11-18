package tachyon.worker;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.accelio.jxio.jxioConnection.JxioConnection;

import tachyon.thrift.ClientBlockInfo;
import tachyon.worker.DataServerTest.DataServerRequest;

public class RDMADataServerRequest implements DataServerRequest{

  /**
   * Create a new jxio connection to the data port and send a block request. The returned value is the
   * response from the server.
   */
  public DataServerMessage request(final ClientBlockInfo block, final long offset,
      final long length) throws IOException {
    try {
      URI uri =
          new URI(String.format("rdma://%s:%d/blockId=%d&offset=%d&length=%d", block.getLocations()
              .get(0).mHost, block.getLocations().get(0).mSecondaryPort, block.blockId, offset,
              length));
      JxioConnection jc = new JxioConnection(uri);
      try {
        InputStream input = jc.getInputStream();
        DataServerMessage recvMsg =
            DataServerMessage.createBlockResponseMessage(false, block.blockId, offset, length);
        recvMsg.recv(input);
        return recvMsg;
      } finally {
        jc.disconnect();
      }
    } catch (URISyntaxException e) {
      throw new IOException("rdma uri could not be resolved");
    }
  }
}
