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

/**
 * Disposer to safely dispose an unneeded {@link BufferEnvelope}.
 */
class Disposer implements BufOwner<Disposer> {
  static final Disposer INSTANCE = new Disposer();

  private Disposer() {
    // prevent instantiation
  }

  void dispose(BufferEnvelope envelope) {
    //noinspection EmptyTryBlock
    try (OwnedByteBuf<Disposer> ignored = envelope.unseal(INSTANCE)) {
      // do nothing
    }
  }

  /**
   * Disposes the buffer in the envelope by unsealing and immediately closing it.
   *
   * @param envelope the envelope to dispose
   * @return always {@code null}
   */
  @Override
  public OwnedByteBuf<Disposer> unseal(BufferEnvelope envelope) {
    dispose(envelope);
    return null;
  }

  @Override
  public Disposer self() {
    return INSTANCE;
  }

  @Override
  public void close() {
    // do nothing as no buffer is actually owned
  }
}
