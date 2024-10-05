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

package alluxio.network.protocol.databuffer.managed;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CheckReturnValue;
import io.netty.buffer.ByteBuf;

import java.lang.ref.WeakReference;

/**
 * An unsafe holder that assumes the caller obeys the ownership rules,
 * and has only minimal runtime overhead to prevent use after move.
 * @param <OwnerT>
 */
public class UnsafeOwnedByteBuf<OwnerT extends BufOwner<OwnerT>> extends OwnedByteBuf<OwnerT>
    implements BufferEnvelope {
  private ByteBuf mBuf;
  private final Class<? extends OwnerT> mOwnerClass;
  private final SharedByteBuf<OwnerT> mSharedBuf;

  // safety: buffer must not have any other live strong references
  protected UnsafeOwnedByteBuf(ByteBuf buffer, Class<? extends OwnerT> ownerClass) {
    Preconditions.checkArgument(buffer.refCnt() == 1);
    mBuf = buffer;
    mOwnerClass = ownerClass;
    mSharedBuf = new SharedByteBuf<>(new WeakReference<>(mBuf), ownerClass);
  }

  /**
   * Creates a buffer envelope from an arbitrary buffer.
   * Very unsafe. Don't use.
   *
   * @param buf buf
   * @return envelope
   */
  @VisibleForTesting
  public static BufferEnvelope unsafeSeal(ByteBuf buf) {
    return new UnsafeOwnedByteBuf<>(buf, UntrackedOwner.class);
  }

  @Override
  public BufferEnvelope send() {
    // safety: the caller must promise not to reuse this buffer after sent
    // this is no-op as move() will be called in unseal
    return this;
  }

  @Override
  public SharedByteBuf<OwnerT> lend() {
    if (mBuf == null) {
      throw new IllegalStateException("use after move, owner was " + mOwnerClass.getName());
    }
    return mSharedBuf;
  }

  @Override
  public ByteBuf unsafeUnwrap() {
    return move();
  }

  @Override
  public void close() {
    // must check if the buffer has moved to avoid double free
    if (mBuf != null) {
      mBuf.release();
      mBuf = null;
    }
  }

  @Override
  public <NewOwnerT extends BufOwner<NewOwnerT>> OwnedByteBuf<NewOwnerT> unseal(NewOwnerT owner) {
    return new UnsafeOwnedByteBuf<>(move(), owner.selfType());
  }

  @CheckReturnValue
  private ByteBuf move() {
    if (mBuf == null) {
      throw new IllegalStateException("use after move, owner was " + mOwnerClass.getName());
    }
    ByteBuf moved = mBuf;
    mBuf = null;
    return moved;
  }
}
