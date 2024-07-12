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

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A holder that tracks historic owners of the buffer.
 * @param <OwnerT> marker type indicating the owning class or function
 */
public class TrackingOwnedByteBuf<OwnerT extends BufOwner<OwnerT>> extends OwnedByteBuf<OwnerT> {
  protected final Class<? extends OwnerT> mOwnerClass;
  private final AtomicReference<ByteBuf> mBuf;
  private final ArrayList<Class<? extends BufOwner<?>>> mHistoricOwners;

  /**
   * Creates a new owned {@link ByteBuf}. The buffer must be freshly allocated and has
   * a reference count of 1.
   * @param buffer the wrapped buffer
   */
  protected TrackingOwnedByteBuf(ByteBuf buffer,
      ArrayList<Class<? extends BufOwner<?>>> historicOwners,
      Class<? extends OwnerT> newOwner) {
    mBuf = new AtomicReference<>(ensureUnique(buffer));
    mHistoricOwners = historicOwners;
    mOwnerClass = newOwner;
    mHistoricOwners.add(mOwnerClass);
  }

  /**
   * Creates a new owner-tracked buffer from a freshly allocated {@link ByteBuf}.
   *
   * @param buffer the buffer. reference count must be 1
   * @param owner the very first owner of this buffer
   * @return the managed buffer
   * @param <OwnerT> owner type
   */
  // todo(bowen): this is unsafe as buffer could already be aliased, even though the ref count is 1
  public static <OwnerT extends BufOwner<OwnerT>>
      TrackingOwnedByteBuf<OwnerT> fromFreshAllocation(ByteBuf buffer, OwnerT owner) {
    return new TrackingOwnedByteBuf<>(ensureUnique(buffer), new ArrayList<>(), owner.selfType());
  }

  @Override
  public BufferEnvelope send() {
    return new Envelope<>(this);
  }

  @Override
  public SharedByteBuf<OwnerT> lend() {
    return new SharedByteBuf<>(new WeakReference<>(share()), mOwnerClass);
  }

  @Override
  public ByteBuf unsafeUnwrap() {
    return move();
  }

  @Override
  public void close() {
    ByteBuf buf = mBuf.getAndSet(null);
    if (buf != null) {
      buf.release();
    }
  }

  private ByteBuf share() {
    final ByteBuf buf = mBuf.get();
    if (buf == null) {
      throwUseAfterMoveException();
    }
    return buf;
  }

  private ByteBuf move() {
    final ByteBuf buf = mBuf.getAndSet(null);
    if (buf == null) {
      throwUseAfterMoveException();
    }
    return buf;
  }

  private void throwUseAfterMoveException() {
    throw new IllegalStateException(
        String.format("attempt to use buffer after ownership transfer, historic owners are "
            + mHistoricOwners.stream()
            .map(Class::getName)
            .collect(Collectors.joining(", "))));
  }

  private static ByteBuf ensureUnique(ByteBuf buf) {
    try {
      Preconditions.checkArgument(buf.refCnt() == 1, "non-unique buffer");
      return buf;
    } catch (Throwable e) {
      buf.release();
      throw e;
    }
  }

  private static class Envelope<OwnerT extends BufOwner<OwnerT>> implements BufferEnvelope {
    private final AtomicReference<ByteBuf> mBufRef;
    private final ArrayList<Class<? extends BufOwner<?>>> mHistoricOwners;

    Envelope(TrackingOwnedByteBuf<OwnerT> buffer) {
      mBufRef = new AtomicReference<>(buffer.move());
      mHistoricOwners = buffer.mHistoricOwners;
    }

    @Override
    public <NewOwnerT extends BufOwner<NewOwnerT>> OwnedByteBuf<NewOwnerT>
        unseal(NewOwnerT newOwner) {
      ByteBuf buf = mBufRef.getAndSet(null);
      if (buf == null) {
        throw new IllegalStateException("unsealing void envelope");
      }
      return new TrackingOwnedByteBuf<>(buf, mHistoricOwners, newOwner.selfType());
    }
  }
}
