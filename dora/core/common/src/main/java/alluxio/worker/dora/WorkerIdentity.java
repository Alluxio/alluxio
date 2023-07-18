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

package alluxio.worker.dora;

import alluxio.wire.WorkerNetAddress;

import com.google.common.hash.PrimitiveSink;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Identity of a worker. Can be used to compare if two workers are equal.
 */
@SuppressWarnings("UnstableApiUsage") // for Guava beta APIs
public final class WorkerIdentity {
  /**
   * Opaque representation of a worker's identity.
   */
  private final byte[] mIdentity;

  private WorkerIdentity(byte[] identity) {
    mIdentity = identity;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkerIdentity that = (WorkerIdentity) o;
    return Arrays.equals(mIdentity, that.mIdentity);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(mIdentity);
  }

  @Override
  public String toString() {
    return "Worker@" + Integer.toHexString(hashCode());
  }

  /**
   * Creates an identity from the network address of the worker.
   *
   * @param workerNetAddress worker's network address
   * @return worker identity
   */
  public static WorkerIdentity definedByNetAddress(WorkerNetAddress workerNetAddress) {
    NetAddressFunnel funnel = NetAddressFunnel.INSTANCE;
    byte[] buffer = new byte[funnel.getNumBytesToFunnel(workerNetAddress)];
    ByteBufferSink sink = new ByteBufferSink(ByteBuffer.wrap(buffer));
    funnel.funnel(workerNetAddress, sink);
    return new WorkerIdentity(buffer);
  }

  /**
   * Funnel that creates a hash for the identity.
   * @see com.google.common.hash.Hasher#putObject(Object, com.google.common.hash.Funnel)
   */
  public enum Funnel implements com.google.common.hash.Funnel<WorkerIdentity> {
    INSTANCE;

    @Override
    public void funnel(WorkerIdentity workerIdentity, PrimitiveSink primitiveSink) {
      primitiveSink.putBytes(workerIdentity.mIdentity);
    }
  }

  /**
   * Funnel that creates a hash from worker net address.
   * @see com.google.common.hash.Hasher#putObject(Object, com.google.common.hash.Funnel)
   */
  public enum NetAddressFunnel implements com.google.common.hash.Funnel<WorkerNetAddress> {
    INSTANCE;

    @Override
    public void funnel(WorkerNetAddress workerNetAddress, PrimitiveSink sink) {
      sink.putUnencodedChars(workerNetAddress.getHost())
          .putUnencodedChars(workerNetAddress.getContainerHost())
          .putUnencodedChars(workerNetAddress.getDomainSocketPath())
          .putInt(workerNetAddress.getRpcPort())
          .putInt(workerNetAddress.getDataPort())
          .putInt(workerNetAddress.getNettyDataPort())
          .putInt(workerNetAddress.getSecureRpcPort())
          .putInt(workerNetAddress.getWebPort());
      // we omit tieredIdentity here as it's not a defining factor of a worker's identity,
      // i.e., two workers with identical hostnames and ports but different tiered identities
      // are considered the same worker
    }

    /**
     * @param workerNetAddress the address to funnel
     * @return the number of bytes that will be put into the sink for the give input
     *         The sink must be able to accept at least this many bytes
     */
    public int getNumBytesToFunnel(WorkerNetAddress workerNetAddress) {
      int hostChars = workerNetAddress.getHost().length();
      int containerHostChars = workerNetAddress.getContainerHost().length();
      int domainSocketPathChars = workerNetAddress.getDomainSocketPath().length();
      return (hostChars + containerHostChars + domainSocketPathChars) * Character.BYTES
          + /* bytes of ports as integers */ Integer.BYTES * 5;
    }
  }

  @SuppressWarnings("NullableProblems") // all arguments must be non-null
  private static class ByteBufferSink implements PrimitiveSink {
    private final ByteBuffer mSink;

    ByteBufferSink(ByteBuffer sink) {
      mSink = sink;
    }

    @Override
    public PrimitiveSink putByte(byte b) {
      mSink.put(b);
      return this;
    }

    @Override
    public PrimitiveSink putBytes(byte[] bytes) {
      mSink.put(bytes);
      return this;
    }

    @Override
    public PrimitiveSink putBytes(byte[] bytes, int off, int len) {
      mSink.put(bytes, off, len);
      return this;
    }

    @Override
    public PrimitiveSink putBytes(ByteBuffer bytes) {
      mSink.put(bytes);
      return this;
    }

    @Override
    public PrimitiveSink putShort(short s) {
      mSink.putShort(s);
      return this;
    }

    @Override
    public PrimitiveSink putInt(int i) {
      mSink.putInt(i);
      return this;
    }

    @Override
    public PrimitiveSink putLong(long l) {
      mSink.putLong(l);
      return this;
    }

    @Override
    public PrimitiveSink putFloat(float f) {
      mSink.putFloat(f);
      return this;
    }

    @Override
    public PrimitiveSink putDouble(double d) {
      mSink.putDouble(d);
      return this;
    }

    @Override
    public PrimitiveSink putBoolean(boolean b) {
      mSink.put(b ? (byte) 1 : (byte) 0);
      return this;
    }

    @Override
    public PrimitiveSink putChar(char c) {
      mSink.putChar(c);
      return this;
    }

    @Override
    public PrimitiveSink putUnencodedChars(CharSequence charSequence) {
      for (int i = 0, len = charSequence.length(); i < len; i++) {
        putChar(charSequence.charAt(i));
      }
      return this;
    }

    @Override
    public PrimitiveSink putString(CharSequence charSequence, Charset charset) {
      return putBytes(charSequence.toString().getBytes(charset));
    }
  }
}
