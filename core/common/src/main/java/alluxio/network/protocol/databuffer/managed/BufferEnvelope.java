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
 * Sealed buffer envelope.
 * <br>
 * There are two ways to obtain an envelope of a buffer:
 * <ol>
 *   <li>Transfer ownership from an existing {@link OwnedByteBuf}
 *       by calling {@link OwnedByteBuf#send()}</li>
 *   <li>Freshly allocate one from a buffer pool</li>
 * </ol>
 *
 * The receiver can obtain ownership of the buffer through {@link #unseal(BufOwner)}.
 * <br>
 * <b>Warning</b>: the only correct way to interact with an envelope is to call
 * {@link #unseal(BufOwner)} as soon as the receiver receives it. Simply discarding it or keeping
 * it as a class field will cause memory leak.
 */
@CheckReturnValue
public interface BufferEnvelope {
  /**
   * Unseals this envelope and grants ownership of the buffer to the owner.
   * After this, the envelope is void and must not be used.
   *
   * @param owner the new owner of this buffer
   * @return unsealed
   * @param <OwnerT> type
   */
  @MustBeClosed
  <OwnerT extends BufOwner<OwnerT>> OwnedByteBuf<OwnerT> unseal(OwnerT owner);

  /**
   * Safely disposes the buffer in the envelope.
   */
  default void dispose() {
    Disposer.INSTANCE.dispose(this);
  }
}
