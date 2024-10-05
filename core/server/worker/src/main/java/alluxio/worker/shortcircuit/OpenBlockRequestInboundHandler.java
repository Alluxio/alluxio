/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.shortcircuit;

import alluxio.client.block.shortcircuit.OpenBlockMessage;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.security.user.ServerUserState;
import alluxio.util.IdUtils;
import alluxio.util.LogUtils;
import alluxio.util.SecurityUtils;
import alluxio.worker.WorkerProcess;
import alluxio.worker.block.BlockWorker;
import io.grpc.Status;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.unix.FileDescriptor;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.SharedSecrets;

import java.io.RandomAccessFile;
import java.util.List;


public class OpenBlockRequestInboundHandler extends ByteToMessageDecoder {

  private static final Logger LOG = LoggerFactory.getLogger(OpenBlockRequestInboundHandler.class);

  private final WorkerProcess mWorkerProcess;
  private RandomAccessFile mBlockFile;
  private final BlockWorker mWorker;
  /**
   * The lock Id of the block being read.
   */
  private long mLockId;
  private AuthenticatedUserInfo mUserInfo;
  private static ThreadLocal<Long> blockIdThreadLocal = new ThreadLocal<>();
  private long mSessionId;
  
  public OpenBlockRequestInboundHandler(WorkerProcess workerProcess) throws UnauthenticatedException {
    this.mWorkerProcess = workerProcess;
    this.mWorker = workerProcess.getWorker(BlockWorker.class);
    this.mLockId = BlockWorker.INVALID_LOCK_ID;
    try {
      AuthenticatedClientUser.set(ServerUserState.global().getUser().getName());
    } catch (UnauthenticatedException e) {
      throw e;
    }
    User proxyUser = AuthenticatedClientUser.getOrNull();
    AuthenticatedClientUser.set(proxyUser);
    this.mUserInfo = getAuthenticatedUserInfo();
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    long blockId = in.readLong();
    boolean isPromote = in.readBoolean();
    blockIdThreadLocal.set(blockId);
    OpenBlockMessage openBlockRequest = new OpenBlockMessage(blockId, isPromote);
    out.add(openBlockRequest);
    if (mLockId == BlockWorker.INVALID_LOCK_ID) {
      mSessionId = IdUtils.createSessionId();
      // TODO(calvin): Update the locking logic so this can be done better
      if (isPromote) {
        try {
          mWorker.moveBlock(mSessionId, blockId, 0);
        } catch (BlockDoesNotExistException e) {
          LOG.debug("Block {} to promote does not exist in Alluxio", blockId, e);
        } catch (Exception e) {
          LOG.warn("Failed to promote block {}: {}", blockId, e.toString());
        }
      }
      mLockId = mWorker.lockBlock(mSessionId, blockId);
      if (mLockId == BlockWorker.INVALID_LOCK_ID) {
        throw new BlockDoesNotExistException(ExceptionMessage.NO_BLOCK_ID_FOUND,
                                             blockId);
      }
      mWorker.accessBlock(mSessionId, blockId);
    } else {
      LOG.warn("Lock block {} without releasing previous block lock {}.",
               blockId, mLockId);
      throw new InvalidWorkerStateException(
          ExceptionMessage.LOCK_NOT_RELEASED.getMessage(mLockId));
    }
    String blockPath = mWorker.getBlockMeta(mSessionId, blockId, mLockId).getPath();
    mBlockFile = new RandomAccessFile(blockPath, "r");
    int blockFd = SharedSecrets.getJavaIOFileDescriptorAccess().get(mBlockFile.getFD());
    FileDescriptor fileDescriptor = new FileDescriptor(blockFd);
    ctx.write(fileDescriptor);
  }

  /**
   * @return {@link AuthenticatedUserInfo} that defines the user that has been authorized
   */
  private AuthenticatedUserInfo getAuthenticatedUserInfo() {
    try {
      if (SecurityUtils.isAuthenticationEnabled(ServerConfiguration.global())) {
        return new AuthenticatedUserInfo(
            AuthenticatedClientUser.getClientUser(ServerConfiguration.global()),
            AuthenticatedClientUser.getConnectionUser(ServerConfiguration.global()),
            AuthenticatedClientUser.getAuthMethod(ServerConfiguration.global()));
      } else {
        return new AuthenticatedUserInfo();
      }
    } catch (Exception e) {
      throw Status.UNAUTHENTICATED.withDescription(e.toString()).asRuntimeException();
    }
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    if (mBlockFile != null) {
      mBlockFile.close();
    }
    AuthenticatedClientUser.remove();
    if (mLockId != BlockWorker.INVALID_LOCK_ID) {
      try {
        mWorker.unlockBlock(mLockId);
      } catch (BlockDoesNotExistException e) {
        LOG.warn("Failed to unlock lock {} of block {} with error {}.",
                 mLockId, blockIdThreadLocal.get(), e.toString());
      }
      mLockId = BlockWorker.INVALID_LOCK_ID;
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LogUtils.warnWithException(LOG, "Exception occurred processing ByteToMessageDecoder : {}.", cause);
    if (mLockId != BlockWorker.INVALID_LOCK_ID) {
      try {
        mWorker.unlockBlock(mLockId);
      } catch (BlockDoesNotExistException e) {
        LOG.warn("Failed to unlock lock {} of block {} with error {}.",
                 mLockId, blockIdThreadLocal.get(), e.toString());
      }
      mLockId = BlockWorker.INVALID_LOCK_ID;
      mWorker.cleanupSession(mSessionId);
    }
    ctx.close();
  }
}
