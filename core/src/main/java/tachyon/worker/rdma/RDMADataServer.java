package tachyon.worker.rdma;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

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
import tachyon.conf.CommonConf;
import tachyon.conf.WorkerConf;
import tachyon.worker.BlocksLocker;
import tachyon.worker.DataServer;
import tachyon.worker.DataServerMessage;

public final class RDMADataServer implements Runnable, DataServer {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int SERVER_INITIAL_BUF_COUNT = 500;
  private static final int SERVER_INC_BUF_COUNT = 50;
  private static final String TRANSPORT = CommonConf.get().JXIO_TRANSPORT;
  // The blocks locker manager.
  private final BlocksLocker mBlocksLocker;
  private final Thread mListenerThread;
  private final EventQueueHandler mEventQueueHandler;
  private final ServerPortal mListener;
  private ArrayList<MsgPool> mMsgPools = new ArrayList<MsgPool>();

  public RDMADataServer(final InetSocketAddress address, final BlocksLocker locker) {
    URI uri = constructRdmaServerUri(address.getHostName(), address.getPort());
    LOG.info("Starting RDMADataServer @ {}", uri);
    mBlocksLocker = locker;
    MsgPool pool =
        new MsgPool(SERVER_INITIAL_BUF_COUNT, 0,
            org.accelio.jxio.jxioConnection.Constants.MSGPOOL_BUF_SIZE);
    mMsgPools.add(pool);
    mEventQueueHandler =
        new EventQueueHandler(new EqhCallbacks(SERVER_INC_BUF_COUNT, 0,
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
      } catch (URISyntaxException  e) {
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
      int lockId = mBlocksLocker.lock(blockId);
      mResponseMessage =
          DataServerMessage.createBlockResponseMessage(true, blockId, offset, length);
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
        } catch (JxioGeneralException e) {
          LOG.error("Exception accured while sending messgae {}", e.toString());
          mSession.discardRequest(m);
        } catch (JxioSessionClosedException e) {
          LOG.error("session was closed unexpectedly {}", e.toString());
          mSession.discardRequest(m);
        }
      }

      if (!mSession.getIsClosing() && mResponseMessage.finishSending()) {
        mSession.close();
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
      return true;
    }
  }

  @Override
  public void run() {
    int ret =
        mEventQueueHandler.runEventLoop(EventQueueHandler.INFINITE_EVENTS,
            EventQueueHandler.INFINITE_DURATION);
    if (ret == -1) {
      LOG.error("{} exception occurred in eventLoop: {}", this.toString(),
          mEventQueueHandler.getCaughtException());
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
    try {
      return new URI(TRANSPORT + "://" + host + ":" + port);
    } catch (URISyntaxException e) {
      LOG.error("could not resolve rdma data server uri, transport type is {}, {}", TRANSPORT,
          e.getCause());
      throw Throwables.propagate(e);
    }
  }
}
