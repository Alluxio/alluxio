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
import com.google.errorprone.annotations.MustBeClosed;

/**
 * Owner of an {@link OwnedByteBuf}.
 * @param <OwnerT>
 */
public interface BufOwner<OwnerT extends BufOwner<OwnerT>> extends AutoCloseable {
  /**
   * Gets self.
   * @return self
   */
  OwnerT self();

  /**
   * Releases the owned buffer.
   */
  @Override
  void close();

  /**
   * Receives the ownership of the buffer in the envelope.
   * This method should only be called with the caller's class.
   *
   * @param envelope the envelope
   * @return the buffer owned by the current owner
   */
  @MustBeClosed
  @CheckReturnValue
  default OwnedByteBuf<OwnerT> unseal(BufferEnvelope envelope) {
    return envelope.unseal(self());
  }

  /**
   * Gets self's concrete type.
   * @return self class
   */
  default Class<? extends OwnerT> selfType() {
    // suppression is safe as long as `self()` is correctly implemented
    @SuppressWarnings("unchecked")
    Class<? extends OwnerT> clazz = (Class<? extends OwnerT>) self().getClass();
    return clazz;
  }
}
