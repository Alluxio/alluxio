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

import com.google.errorprone.annotations.CheckReturnValue;
import io.netty.buffer.ByteBuf;

/**
 * A holder that ensures it's the sole owner of the held ByteBuf.
 *
 * This is made an abstract class instead of an interface to prevent subclasses doing something
 * like:
 * <pre>
 *   class SmartByteBuf extends ByteBuf implements OwnedByteBuf {}
 * </pre>
 * and breaking the non-aliasing rule by obtaining a reference to ByteBuf.
 * @param <OwnerT> marker type indicating the owning class or function
 */
public abstract class OwnedByteBuf<OwnerT extends BufOwner<OwnerT>> implements AutoCloseable {
  /**
   * Puts the buffer into an envelope so that the ownership can be transferred to a receiver.
   * The receiver should call {@link BufferEnvelope#unseal(BufOwner)} ()} to claim ownership.
   * After the ownership transfer, this wrapper should not be used anymore in any way,
   * and any call on this buffer's methods will throw exception.
   *
   * @return buffer with transferred ownership
   */
  @CheckReturnValue // the envelope must be unsealed by a receiver otherwise the buffer is leaked
  public abstract BufferEnvelope send();

  /**
   * Temporarily lends this buffer to a borrower.
   * @return a shared buffer
   */
  public abstract SharedByteBuf<OwnerT> lend();

  /**
   * Unwraps this wrapper and exposes the underlying buffer, ending the ownership management.
   * Attempts to use this wrapper after this method is called will throw exception.
   * This method should only be called at the boundary where the buffer needs to be handed
   * over to a component that does its own buffer management.
   * <br>
   * <b>WARNING: retaining the raw buffer could lead to memory leaks!</b>
   * @return the underlying buffer
   */
  @CheckReturnValue
  public abstract ByteBuf unsafeUnwrap();

  /**
   * Ends ownership management, releasing the wrapped buffer if this wrapper still owns it,
   * or doing nothing otherwise. Calling this method on a moved object has no effect.
   */
  @Override
  public abstract void close();
}
