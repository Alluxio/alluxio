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
 * Placeholder class when owner tracking is not needed.
 */
class UntrackedOwner implements BufOwner<UntrackedOwner> {
  private static final UntrackedOwner INSTANCE = new UntrackedOwner();

  private UntrackedOwner() {
    // prevent instantiation
  }

  @Override
  public OwnedByteBuf<UntrackedOwner> unseal(BufferEnvelope envelope) {
    throw new IllegalStateException("untracked owner cannot actually own a buffer");
  }

  @Override
  public UntrackedOwner self() {
    return INSTANCE;
  }

  @Override
  public void close() {
    // do nothing
  }
}
