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

import io.netty.buffer.ByteBuf;

import java.lang.ref.WeakReference;

/**
 * A shared buffer.
 * @param <OwnerT>
 */
public class SharedByteBuf<OwnerT extends BufOwner<OwnerT>> /* todo(bowen): extends ByteBuf */ {
  private final Class<? extends BufOwner<?>> mOwnerClass;
  private final WeakReference<ByteBuf> mBuf;

  protected SharedByteBuf(WeakReference<ByteBuf> bufRef,
      Class<? extends OwnerT> ownerClass) {
    mBuf = bufRef;
    mOwnerClass = ownerClass;
  }
}
