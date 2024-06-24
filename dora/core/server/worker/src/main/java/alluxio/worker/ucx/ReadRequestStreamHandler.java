package alluxio.worker.ucx;

import alluxio.AlluxioURI;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PageNotFoundException;
import alluxio.exception.runtime.UnknownRuntimeException;
import alluxio.proto.dataserver.Protocol;

import com.google.protobuf.InvalidProtocolBufferException;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * (Deprecated, unused now)
 * Handling ReadRequest with UCX Stream API.
 */
public class ReadRequestStreamHandler implements UcxRequestHandler {
  private static final Logger LOG = LoggerFactory.getLogger(UcpServer.class);
  Protocol.ReadRequest mReadRequest = null;
  UcpEndpoint mRemoteEp;

  public ReadRequestStreamHandler() {
  }

  @Override
  public void handle(UcxMessage message, UcxConnection remoteConnection) {
    mRemoteEp = remoteConnection.getEndpoint();
    CacheManager cacheManager = null;
    try {
      mReadRequest = Protocol.ReadRequest.parseFrom(message.getRPCMessage());
      cacheManager = CacheManager.Factory.get(Configuration.global());
    } catch (InvalidProtocolBufferException e) {
      throw new UnknownRuntimeException("Error in parsing ReadRequest protobuf.");
    } catch (IOException e) {
      throw new UnknownRuntimeException("Error getting CacheManager instance.");
    }
    final String fileId =
        new AlluxioURI(mReadRequest.getOpenUfsBlockOptions().getUfsPath()).hash();
    long offset = mReadRequest.getOffset();
    long totalLength = mReadRequest.getLength();
    long pageSize = Configuration.global().getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE);
    List<UcpRequest> requests = new ArrayList<>();
    for (int bytesRead = 0; bytesRead < totalLength; ) {
      int pageIndex = (int)(offset / pageSize);
      int pageOffset = (int)(offset % pageSize);
      int readLen = (int)Math.min(totalLength - bytesRead, pageSize - pageOffset);
      PageId pageId = new PageId(fileId, pageIndex);
      try {
        Optional<UcpMemory> readContentUcpMem =
            cacheManager.getUcpMemory(pageId, pageOffset, readLen);
        if (!readContentUcpMem.isPresent()) {
          break;
        }
        offset += readLen;
        bytesRead += readLen;
        // first 8 bytes -> sequence  second 8 bytes -> size
        ByteBuffer preamble = ByteBuffer.allocateDirect(16);
        preamble.clear();
        long seq = remoteConnection.getNextSequence();
        preamble.putLong(seq);
        preamble.putLong(readContentUcpMem.get().getLength());
        preamble.clear();
        UcpRequest preambleReq = mRemoteEp.sendStreamNonBlocking(UcxUtils.getAddress(preamble),
            16, new UcxCallback() {
              public void onSuccess(UcpRequest request) {
                LOG.debug("preamble sent, sequence:{}, len:{}"
                    ,seq, readContentUcpMem.get().getLength());
              }

              public void onError(int ucsStatus, String errorMsg) {
                LOG.error("error sending preamble:pageoffset:{}:readLen:{}",
                    pageOffset, readLen);
              }
            });
        requests.add(preambleReq);

        long[] addrs = new long[2];
        long[] sizes = new long[2];
        ByteBuffer seqBuf = ByteBuffer.allocateDirect(8);
        seqBuf.putLong(seq); seqBuf.clear();
        addrs[0] = UcxUtils.getAddress(seqBuf); sizes[0] = 8;
        addrs[1] = readContentUcpMem.get().getAddress(); sizes[1] = readContentUcpMem.get().getLength();
        UcpRequest sendReq = mRemoteEp.sendStreamNonBlocking(
            addrs, sizes, new UcxCallback() {
              public void onSuccess(UcpRequest request) {
                LOG.debug("send complete for pageoffset:{}:readLen:{}",
                    pageOffset, readLen);
                readContentUcpMem.get().deregister();
              }

              public void onError(int ucsStatus, String errorMsg) {
                LOG.error("error sending :pageoffset:{}:readLen:{}",
                    pageOffset, readLen);
                readContentUcpMem.get().deregister();
              }
            });
        requests.add(sendReq);
      } catch (PageNotFoundException | IOException e) {
        throw new RuntimeException(e);
      }
    }// end for
    LOG.debug("Handle read req:{} complete", mReadRequest);
    while (requests.stream().anyMatch(r -> !r.isCompleted())) {
      LOG.debug("Wait for all {} ucpreq to complete, sleep for 5 sec...", requests.size());
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
