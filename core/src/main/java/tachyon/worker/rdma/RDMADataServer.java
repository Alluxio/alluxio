/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.rdma;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;

import com.google.common.base.Throwables;

import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.NameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.accelio.jxio.EventName;
import org.accelio.jxio.EventQueueHandler;
import org.accelio.jxio.EventReason;
import org.accelio.jxio.Msg;
import org.accelio.jxio.MsgPool;
import org.accelio.jxio.ServerPortal;
import org.accelio.jxio.ServerSession;
import org.accelio.jxio.ServerSession.SessionKey;
import org.accelio.jxio.WorkerCache.Worker;
import org.accelio.jxio.exceptions.JxioGeneralException;
import org.accelio.jxio.exceptions.JxioSessionClosedException;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.worker.BlocksLocker;
import tachyon.worker.DataServer;
import tachyon.worker.DataServerMessage;
import tachyon.worker.hierarchy.StorageDir;

public final class RDMADataServer implements Runnable, DataServer {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private int mServerInitialBufferCount = 500;
  private int mServerIncBufCount = 50;
  // The blocks locker manager.
  private final BlocksLocker mBlocksLocker;
  private final Thread mListenerThread;
  private final EventQueueHandler mEventQueueHandler;
  private final ServerPortal mListener;
  private final TachyonConf mConf;
  private ArrayList<MsgPool> mMsgPools = new ArrayList<MsgPool>();
  private boolean mClose = false;

  public RDMADataServer(final InetSocketAddress address, final BlocksLocker locker,
      TachyonConf conf) {
    mConf = conf;
    mBlocksLocker = locker;
    URI uri = constructRdmaServerUri(address.getHostName(), address.getPort());
    LOG.info("Starting RDMADataServer @ {}", uri);
    mServerIncBufCount = mConf.getInt(Constants.WORKER_RDMA_SERVER_INC_BUFFERS, mServerIncBufCount);
    mServerInitialBufferCount =
        mConf.getInt(Constants.WORKER_RDMA_SERVER_INITIAL_BUFFERS, mServerInitialBufferCount);
    MsgPool pool =
        new MsgPool(mServerInitialBufferCount, 0,
            org.accelio.jxio.jxioConnection.Constants.MSGPOOL_BUF_SIZE);
    mMsgPools.add(pool);
    mEventQueueHandler =
        new EventQueueHandler(new EqhCallbacks(mServerIncBufCount, 0,
            org.accelio.jxio.jxioConnection.Constants.MSGPOOL_BUF_SIZE));
    mEventQueueHandler.bindMsgPool(pool);
    mListener = new ServerPortal(mEventQueueHandler, uri, new PortalServerCallbacks(), null);
    mListenerThread = new Thread(this);
    mListenerThread.start();
  }

  /**
   * Callbacks for the listener server portal
   */
  private final class PortalServerCallbacks implements ServerPortal.Callbacks {

    public void onSessionEvent(EventName session_event, EventReason reason) {
      LOG.debug("got event {} because of {}", session_event.toString(), reason.toString());
      if (session_event == EventName.PORTAL_CLOSED) {
        mEventQueueHandler.breakEventLoop();
      }
    }

    public void onSessionNew(SessionKey sesKey, String srcIP, Worker workerHint) {
      LOG.info("onSessionNew {}", sesKey.getUri());
      URI uri;
      try {
        uri = new URI(sesKey.getUri());
      } catch (URISyntaxException e) {
        mListener.reject(sesKey, EventReason.ADDR_ERROR, "Client uri could not be resolved");
        return;
      }
      SessionServerCallbacks callbacks = new SessionServerCallbacks(uri);
      ServerSession session = new ServerSession(sesKey, callbacks);
      callbacks.setSession(session);
      mListener.accept(session);
    }
  }

  private final class SessionServerCallbacks implements ServerSession.Callbacks {
    private final DataServerMessage mResponseMessage;
    private ServerSession mSession;

    public SessionServerCallbacks(URI uri) {
      List<NameValuePair> params = URLEncodedUtils.parse(uri, "UTF-8");
      long blockId = Long.parseLong(params.get(0).getValue());
      long offset = Long.parseLong(params.get(1).getValue());
      long length = Long.parseLong(params.get(2).getValue());
      LOG.debug("got request for block id {} with offset {} and length {}",
          blockId, offset, length);
      final int lockId = mBlocksLocker.getLockId();
      final StorageDir storageDir = mBlocksLocker.lock(blockId, lockId);
      ByteBuffer data;
      int dataLen = 0;
      try {
        data = storageDir.getBlockData(blockId, offset, (int) length);
        storageDir.accessBlock(blockId);
        dataLen = data.limit();
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        data = null;
      }
      mResponseMessage =
          DataServerMessage.createBlockResponseMessage(true, blockId, offset, dataLen, data);
      mResponseMessage.setLockId(lockId);
    }

    public void setSession(ServerSession ses) {
      mSession = ses;
    }

    public void onRequest(Msg m) {
      if (mSession.getIsClosing()) {
        mSession.discardRequest(m);
      } else {
        mResponseMessage.copyMsgToBuffer(m.getOut());
        try {
          mSession.sendResponse(m);
          if (mResponseMessage.finishSending()) {
            mSession.close();
          }
        } catch (JxioGeneralException e) {
          LOG.error("Exception accured while sending message {}", e.toString());
          mSession.close();
          mSession.discardRequest(m);
        } catch (JxioSessionClosedException e) {
          LOG.error("session was closed unexpectedly {}", e.toString());
          mSession.close();
          mSession.discardRequest(m);
        }
      }
    }

    public void onSessionEvent(EventName session_event, EventReason reason) {
      LOG.debug("got event {}, the reason is {}", session_event.toString(), reason.toString());
      if (session_event == EventName.SESSION_CLOSED) {
        mResponseMessage.close();
        mBlocksLocker.unlock(Math.abs(mResponseMessage.getBlockId()), mResponseMessage.getLockId());
      }
    }

    public boolean onMsgError(Msg msg, EventReason reason) {
      LOG.error("{} onMsgErrorCallback. reason is {}", this.toString(), reason);
      mSession.close();
      return true;
    }
  }

  @Override
  public void run() {
    while (!mClose) {
      int ret =
          mEventQueueHandler.runEventLoop(EventQueueHandler.INFINITE_EVENTS,
              EventQueueHandler.INFINITE_DURATION);
      if (ret == -1) {
        LOG.error("{} exception occurred in eventLoop: {}", this.toString(),
            mEventQueueHandler.getCaughtException());
      }
    }
    mEventQueueHandler.stop();
    mEventQueueHandler.close();
    for (MsgPool mp : mMsgPools) {
      mp.deleteMsgPool();
    }
    mMsgPools.clear();
  }

  @Override
  public void close() {
    LOG.info("closing server");
    mClose = true;
    mEventQueueHandler.breakEventLoop();
  }

  @Override
  public boolean isClosed() {
    return mListener.getIsClosing();
  }

  private final class EqhCallbacks implements EventQueueHandler.Callbacks {
    private final int mNumMsgs;
    private final int mInMsgSize;
    private final int mOutMsgSize;

    public EqhCallbacks(int msgs, int in, int out) {
      mNumMsgs = msgs;
      mInMsgSize = in;
      mOutMsgSize = out;
    }

    public MsgPool getAdditionalMsgPool(int in, int out) {
      MsgPool mp = new MsgPool(mNumMsgs, mInMsgSize, mOutMsgSize);
      mMsgPools.add(mp);
      return mp;
    }
  }

  @Override
  public int getPort() {
    return mListener.getUri().getPort();
  }

  private URI constructRdmaServerUri(String host, int port) {
    String transport = mConf.get(Constants.JXIO_TRANSPORT, "rdma");
    try {
      return new URI(transport + "://" + host + ":" + port);
    } catch (URISyntaxException e) {
      LOG.error("could not resolve rdma data server uri, transport type is {}, {}", transport,
          e.getCause());
      throw Throwables.propagate(e);
    }
  }
}
