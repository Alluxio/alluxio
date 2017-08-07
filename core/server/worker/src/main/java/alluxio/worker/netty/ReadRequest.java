package alluxio.worker.netty;

import alluxio.util.IdUtils;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Represents a read request received from netty channel.
 */
@ThreadSafe
class ReadRequest {
  private final long mId;
  private final long mStart;
  private final long mEnd;
  private final long mPacketSize;
  private final long mSessionId;

  ReadRequest(long id, long start, long end, long packetSize) {
    mId = id;
    mStart = start;
    mEnd = end;
    mPacketSize = packetSize;
    mSessionId = IdUtils.createSessionId();
  }

  /**
   * @return session Id
   */
  public long getSessionId() {
    return mSessionId;
  }

  /**
   * @return block id of the read request
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the start offset in bytes of this read request
   */
  public long getStart() {
    return mStart;
  }

  /**
   * @return the end offset in bytes of this read request
   */
  public long getEnd() {
    return mEnd;
  }

  /**
   * @return the packet size in bytes of this read request
   */
  public long getPacketSize() {
    return mPacketSize;
  }
}
