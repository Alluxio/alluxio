package alluxio.worker.ucx;

import alluxio.AlluxioURI;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.concurrent.jsr.CompletableFuture;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PageNotFoundException;
import alluxio.exception.runtime.UnknownRuntimeException;
import alluxio.proto.dataserver.Protocol;
import alluxio.network.ucx.AlluxioUcxUtils;
import alluxio.util.io.ByteBufferInputStream;
import alluxio.util.io.ByteBufferOutputStream;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpRemoteKey;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucs.UcsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class ReadRequestRMAHandler implements UcxRequestHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ReadRequestRMAHandler.class);
  /* Current Work flow: client initiate RMAReadRequest msg,
  client prepare local mem region, send the rpc req thru tag API,
  server do PUT to client mem region.
   */

  @Override
  public void handle(UcxMessage message, UcxConnection remoteConnection) {
    CacheManager cacheManager = null;
    try {
      cacheManager = CacheManager.Factory.get(Configuration.global());
    } catch (IOException e) {
      throw new UnknownRuntimeException("Error getting CacheManager instance.");
    }
    UcpEndpoint remoteEp = remoteConnection.getEndpoint();
    ByteBuffer infoBuffer = message.getRPCMessage();
    long remoteMemAddress;
    UcpRemoteKey remoteRKey;
    Protocol.ReadRequestRMA readRequest;
    try (ByteBufferInputStream bbis = ByteBufferInputStream.getInputStream(infoBuffer)) {
      //  RPC message contains:
      //  client remote mem addr (long) | client remote mem addr Rkey buffer
      //  ReadRequest protobuf
      readRequest = Protocol.ReadRequestRMA.parseFrom(infoBuffer.duplicate());
      remoteMemAddress = readRequest.getRemoteMemAddr();
      byte[] rkeyBufBytes = readRequest.getRkeyBuf().toByteArray();
      ByteBuffer rkeyBuf = ByteBuffer.allocateDirect(rkeyBufBytes.length);
      rkeyBuf.put(rkeyBufBytes);
      rkeyBuf.clear();
      remoteRKey = remoteEp.unpackRemoteKey(rkeyBuf);
    } catch (IOException e) {
      LOG.error("Exception in parsing RMA Read Request:", e);
      throw new RuntimeException(e);
    }

    final String fileId =
        new AlluxioURI(readRequest.getOpenUfsBlockOptions().getUfsPath()).hash();
    long offset = readRequest.getOffset();
    long totalLength = readRequest.getLength();
    long pageSize = Configuration.global().getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE);
    long remoteAddrPosition = remoteMemAddress;

    String errMsg = "";
    AsyncFuture<String> asyncFuture = new AsyncFuture<>();
    int totalRequests = 0;
    int bytesRead = 0;
    for (; bytesRead < totalLength; ) {
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
        LOG.debug("PUT-ing to remoteAddr:{}", remoteAddrPosition);
        UcpRequest putRequest = remoteEp.putNonBlocking(readContentUcpMem.get().getAddress(),
            readContentUcpMem.get().getLength(), remoteAddrPosition,
            remoteRKey, new UcxCallback() {
              public void onSuccess(UcpRequest request) {
                LOG.debug("onSuccess put pageid:{}:pageOffset:{}:len:{}",
                    pageId, pageOffset, readLen);
                asyncFuture.complete(String.format("pageid:%s:pageOffset:%d:len:%d",
                    pageId.toString(), pageOffset, readLen));
                readContentUcpMem.get().deregister();
              }

              public void onError(int ucsStatus, String errorMsg) {
                LOG.error("onError put pageid:{}:pageOffset:{}:len:{}"
                        + " ucsStatus:{}:errMsg:{}",
                    pageId, pageOffset, readLen, ucsStatus, errorMsg);
                asyncFuture.fail(new UcxException(errorMsg));
                readContentUcpMem.get().deregister();
              }
            });
        totalRequests += 1;
        offset += readLen;
        bytesRead += readLen;
        remoteAddrPosition += readLen;
        if (readLen < readContentUcpMem.get().getLength()) {
          LOG.warn("Read requested length not fulfilled, readlen:{}, bytesRead:{}",
              totalLength, bytesRead);
          break;
        }
      } catch (PageNotFoundException | IOException ex) {
        LOG.error("Page not found for pageId:{}", pageId);
        break;
      }
    } // end for
    LOG.debug("Handle RMA read req:{} complete, blockingly wait for all RMA PUT to compelte",
        readRequest);
    asyncFuture.setTotalExpected(totalRequests);
    try {
      asyncFuture.get();
    } catch (Throwable e) {
      errMsg = String.format("Error during read ufsPath:%s:fileId:%s:offset:%s:length:%s, exception:%s",
          readRequest.getOpenUfsBlockOptions().getUfsPath(),
          fileId, offset, totalLength, e.getMessage());
      LOG.error(errMsg);
    }

    LOG.debug("All PUT request completed, notifying client...");
    Protocol.ReadResponseRMA.Builder readResponseRMABuilder = Protocol.ReadResponseRMA.newBuilder()
        .setReadLength(bytesRead);
    if (StringUtils.isNotEmpty(errMsg)) {
      readResponseRMABuilder.setErrorMsg(errMsg);
    }
    byte[] responseBytes = readResponseRMABuilder.build().toByteArray();
    UcxMessage replyMessage = new UcxMessage(message.getMessageId(),
        UcxMessage.Type.Reply,
        ByteBuffer.wrap(responseBytes));
    UcpMemory relyMemoryBlock = UcxMemoryPool.allocateMemory(AlluxioUcxUtils.METADATA_SIZE_COMMON,
        UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST);
    ByteBuffer replyBuffer = UcxUtils.getByteBufferView(relyMemoryBlock.getAddress(),
        relyMemoryBlock.getLength());
    try (ByteBufferOutputStream bbos = ByteBufferOutputStream.getOutputStream(replyBuffer)) {
      UcxMessage.toByteBuffer(replyMessage, bbos);
      replyBuffer.clear();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    CompletableFuture<Boolean> completed = new CompletableFuture<>();
    UcpRequest req = remoteConnection.getEndpoint().sendStreamNonBlocking(
        replyBuffer, new UcxCallback() {
      public void onSuccess(UcpRequest request) {
        relyMemoryBlock.deregister();
        completed.complete(true);
      }

      public void onError(int ucsStatus, String errorMsg) {
        completed.complete(false);
        relyMemoryBlock.deregister();
        throw new UcxException(errorMsg);
      }
    });
    LOG.debug("Blockingly wait for completion reply msg sending...");
    boolean completeSending = false;
    try {
      completeSending = completed.get();
      if (!completeSending) {
        throw new UnknownRuntimeException(String.format("Error sending completion reply after handling ucxmsg:%s",
            message));
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new UnknownRuntimeException(String.format("Error sending compeletion reply after handling ucxmsg:%s",
          message));
    }
  }
}
