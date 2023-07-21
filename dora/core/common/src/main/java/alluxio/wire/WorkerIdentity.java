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

package alluxio.wire;

import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.UnsafeByteOperations;
import org.apache.commons.codec.binary.Hex;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import javax.annotation.concurrent.Immutable;

/**
 * An identifier that uniquely identifies a worker.
 * The associated utility class {@link Parsers} provides means of converting between a GRPC
 * representation and an instance of this class.
 */
@Immutable
public final class WorkerIdentity {
  private final byte[] mId;
  private final int mVersion;
  private int mHashcode = 0;

  WorkerIdentity(byte[] id, int version) {
    mId = id;
    mVersion = version;
  }

  /**
   * Converts from a Protobuf representation.
   *
   * @param proto the protobuf message
   * @return the worker identity
   * @throws ProtoParsingException when the protobuf message cannot be parsed
   */
  public static WorkerIdentity fromProto(alluxio.grpc.WorkerIdentity proto)
      throws ProtoParsingException {
    return Parsers.fromProto(proto);
  }

  /**
   * Converts to Protobuf representation.
   *
   * @return this identity represented in Protobuf message
   */
  public alluxio.grpc.WorkerIdentity toProto() {
    return Parsers.toProto(this);
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
    return mVersion == that.mVersion
        && Arrays.equals(mId, that.mId);
  }

  @Override
  public int hashCode() {
    int h = mHashcode;
    if (h == 0 && mId.length > 0) {
      h = Arrays.hashCode(mId);
      h = h * 31 + mVersion;
      mHashcode = h;
    }
    return mHashcode;
  }

  @Override
  public String toString() {
    return String.format("Worker (%s)",
        Parsers.getParser(mVersion).getVersionSpecificRepresentation(this));
  }

  public static class Parsers {
    public static WorkerIdentity fromProto(alluxio.grpc.WorkerIdentity proto)
        throws ProtoParsingException {
      if (!proto.hasVersion()) {
        Descriptors.FieldDescriptor descriptor = proto.getDescriptorForType()
            .findFieldByNumber(alluxio.grpc.WorkerIdentity.VERSION_FIELD_NUMBER);
        throw new MissingRequiredFieldsParsingException(descriptor, proto);
      }
      final int version = proto.getVersion();
      final Parser parser;
      try {
        parser = getParser(version);
      } catch (IllegalArgumentException e) {
        throw new InvalidVersionParsingException(version, 0, 1);
      }
      return parser.fromProto(proto);
    }

    public static alluxio.grpc.WorkerIdentity toProto(WorkerIdentity workerIdentity) {
      return getParser(workerIdentity.mVersion).toProto(workerIdentity);
    }

    static Parser getParser(int version) throws IllegalArgumentException {
      final Parser parser;
      switch (version) {
        case 0:
          parser = ParserV0.INSTANCE;
          break;
        case 1:
          parser = ParserV1.INSTANCE;
          break;
        default:
          throw new IllegalArgumentException("Unrecognized version: " + version);
      }
      return parser;
    }
  }

  abstract static class Parser {
    protected abstract int getVersion();

    protected abstract byte[] parseIdentifier(ByteString identifier)
        throws ProtoParsingException;

    /**
     * @return a version-specific string representation of worker identity
     */
    public Object getVersionSpecificRepresentation(WorkerIdentity identity) {
      ensureVersionMatch(identity);
      return getVersionSpecificRepresentation0(identity);
    }

    protected abstract Object getVersionSpecificRepresentation0(WorkerIdentity identity);

    protected void ensureVersionMatch(WorkerIdentity identity) throws IllegalArgumentException {
      if (identity.mVersion != getVersion()) {
        throw new IllegalArgumentException(
            String.format("Identity definition version mismatch, expected %s, got %s",
                getVersion(), identity.mVersion));
      }
    }

    /**
     * Converts to a protobuf representation.
     *
     * @param workerIdentity the object to convert
     * @return worker identity
     */
    public alluxio.grpc.WorkerIdentity toProto(WorkerIdentity workerIdentity) {
      alluxio.grpc.WorkerIdentity.Builder builder = alluxio.grpc.WorkerIdentity.newBuilder();
      return builder
          .setVersion(workerIdentity.mVersion)
          .setIdentifier(
              // this is safe as the identity object is immutable
              UnsafeByteOperations.unsafeWrap(workerIdentity.mId))
          .build();
    }

    /**
     * Parses from a Protobuf representation.
     *
     * @param workerIdentityProto the proto message
     * @return the worker identity
     * @throws MissingRequiredFieldsParsingException if the version or the identifier fields are
     *         missing in the protobuf representation
     * @throws InvalidVersionParsingException if the version is not {@link #getVersion()}
     * @throws ProtoParsingException if the protobuf representation is invalid
     */
    public WorkerIdentity fromProto(alluxio.grpc.WorkerIdentity workerIdentityProto)
        throws ProtoParsingException {
      if (!workerIdentityProto.hasVersion()) {
        Descriptors.FieldDescriptor descriptor = workerIdentityProto.getDescriptorForType()
            .findFieldByNumber(alluxio.grpc.WorkerIdentity.VERSION_FIELD_NUMBER);
        throw new MissingRequiredFieldsParsingException(descriptor, workerIdentityProto);
      }
      final int version = workerIdentityProto.getVersion();
      if (version != getVersion()) {
        throw new InvalidVersionParsingException(version, getVersion());
      }
      if (!workerIdentityProto.hasIdentifier()) {
        Descriptors.FieldDescriptor descriptor = workerIdentityProto.getDescriptorForType()
            .findFieldByNumber(alluxio.grpc.WorkerIdentity.IDENTIFIER_FIELD_NUMBER);
        throw new MissingRequiredFieldsParsingException(descriptor, workerIdentityProto);
      }
      return new WorkerIdentity(parseIdentifier(workerIdentityProto.getIdentifier()), version);
    }
  }

  /**
   * Parser for legacy long-based worker IDs.
   * <br>
   * Code that needs to be forward- and backward-compatible should not rely on a version
   * specific parser implementation.
   */
  public static class ParserV0 extends Parser {
    public static final ParserV0 INSTANCE = new ParserV0();
    public static final int VERSION = 0;
    private static final int ID_LENGTH_BYTES = 8; // long

    private ParserV0() { } // singleton

    @Override
    public int getVersion() {
      return VERSION;
    }

    @Override
    protected byte[] parseIdentifier(ByteString identifier)
        throws ProtoParsingException {
      byte[] idBytes = identifier.toByteArray();
      if (idBytes.length != ID_LENGTH_BYTES) {
        throw new ProtoParsingException(String.format(
            "Invalid identifier length: %s, expecting %s. identifier: %s",
            identifier.size(), ID_LENGTH_BYTES, Hex.encodeHexString(idBytes)));
      }
      return idBytes;
    }

    @Override
    protected Long getVersionSpecificRepresentation0(WorkerIdentity workerIdentity) {
      return Longs.fromByteArray(workerIdentity.mId);
    }

    /**
     * Parses from a long worker ID.
     * <br>
     * Code that needs to be forward- and backward-compatible should not rely on a version
     * specific parser implementation.
     *
     * @param workerId worker ID
     * @return worker identity
     */
    public WorkerIdentity fromLong(long workerId) {
      byte[] id = Longs.toByteArray(workerId);
      return new WorkerIdentity(id, VERSION);
    }

    /**
     * Converts to a numeric worker ID represented as a {@code long}.
     *
     * @param identity identity
     * @return numeric worker ID
     * @throws IllegalArgumentException if the identity was not created from a numeric ID
     */
    public long toLong(WorkerIdentity identity) throws IllegalArgumentException {
      ensureVersionMatch(identity);
      return getVersionSpecificRepresentation0(identity);
    }
  }

  /**
   * Parser for {@link WorkerIdentity} that is based on a UUID.
   * <br>
   * Code that needs to be forward- and backward-compatible should not rely on a version
   * specific parser implementation.
   */
  public static class ParserV1 extends Parser {
    public static final ParserV1 INSTANCE = new ParserV1();
    public static final int VERSION = 1;

    private static final int ID_LENGTH_BYTES = 16; // UUID uses 128 bits

    private ParserV1() { } // singleton

    @Override
    public int getVersion() {
      return VERSION;
    }

    @Override
    protected byte[] parseIdentifier(ByteString identifier)
        throws ProtoParsingException {
      if (identifier.size() != ID_LENGTH_BYTES) {
        throw new ProtoParsingException(String.format(
            "Invalid identifier length: %s, expecting %s. identifier: %s",
            identifier.size(), ID_LENGTH_BYTES, Hex.encodeHexString(identifier.toByteArray())));
      }
      return identifier.toByteArray();
    }

    @Override
    protected UUID getVersionSpecificRepresentation0(WorkerIdentity identity) {
      ByteBuffer buffer = ByteBuffer.wrap(identity.mId);
      long msb = buffer.getLong();
      long lsb = buffer.getLong();
      return new UUID(msb, lsb);
    }

    /**
     * Parses from a UUID representation.
     * <br>
     * Note that the identity being a UUID is an
     * implementation detail that is specific to version 1 of the definition of a worker identity.
     * Code that needs to be backwards-compatible should not rely on a version
     * specific parser implementation.
     *
     * @param uuid the uuid representing the worker identity
     * @return the worker identity
     */
    public WorkerIdentity fromUUID(UUID uuid) {
      ByteBuffer buffer = ByteBuffer.allocate(ID_LENGTH_BYTES);
      // The bytes are encoded in big endian
      buffer.putLong(uuid.getMostSignificantBits());
      buffer.putLong(uuid.getLeastSignificantBits());
      byte[] bytes = buffer.array();
      return new WorkerIdentity(bytes, VERSION);
    }

    /**
     * Parses a worker identity from a string representation of a UUID.
     * <br>
     * Note that the identity being a UUID is an
     * implementation detail that is specific to version 1 of the definition of a worker identity.
     * Code that needs to be backwards-compatible should not rely on a version
     * specific parser implementation.
     *
     * @param uuid the string representation of the UUID that is used as the worker's identity
     * @return worker identity
     * @throws IllegalArgumentException if the string is not a valid representation of a UUID
     * @see #fromUUID(UUID)
     */
    public WorkerIdentity fromUUID(String uuid) throws IllegalArgumentException {
      UUID id = UUID.fromString(uuid);
      return fromUUID(id);
    }

    /**
     * Converts to a UUID.
     *
     * @param identity the worker identity
     * @return identity represented as a UUID
     * @throws IllegalArgumentException if the identity was not created from a UUID
     */
    public UUID toUUID(WorkerIdentity identity) throws IllegalArgumentException {
      ensureVersionMatch(identity);
      return getVersionSpecificRepresentation0(identity);
    }
  }
}
