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

import alluxio.exception.status.InvalidArgumentException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.util.StdConverter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.google.common.primitives.Longs;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.annotations.JsonAdapter;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.UnsafeByteOperations;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * An identifier that uniquely identifies a worker.
 * The associated utility class {@link Parsers} provides means of converting between a GRPC
 * representation and an instance of this class.
 */
@Immutable
@JsonAdapter(WorkerIdentity.GsonSerde.class)
@JsonDeserialize(converter = WorkerIdentity.JacksonDeserializeConverter.class)
@JsonSerialize(using = WorkerIdentity.JacksonSerializer.class)
public final class WorkerIdentity implements Serializable {
  private static final long serialVersionUID = -3241509286694514179L;

  private final byte[] mId;
  private final int mVersion;
  private transient int mHashcode;

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
    int hashCode = mHashcode;
    if (hashCode == 0) {
      hashCode = Arrays.hashCode(mId) * 31 + mVersion;
      mHashcode = hashCode;
    }
    return hashCode;
  }

  /**
   * Construct from a workerid representation from registration.
   *
   * @param workerIdentityStr
   * @return WorkerIdentity object
   * @throws InvalidArgumentException
   */
  public static WorkerIdentity fromString(String workerIdentityStr)
      throws InvalidArgumentException {
    String prefix = "worker-";
    String errStr = "Unrecognized worker identity string";
    if (!workerIdentityStr.startsWith(prefix)) {
      throw new InvalidArgumentException(errStr + ": " + workerIdentityStr);
    }
    String idStr = workerIdentityStr.substring(prefix.length());
    try {
      return ParserV1.INSTANCE.fromUUID(idStr);
    } catch (java.lang.IllegalArgumentException ex) {
      // DO NOTHING
    }
    try {
      return ParserV0.INSTANCE.fromLong(Long.parseLong(idStr));
    } catch (NumberFormatException ex) {
      throw new InvalidArgumentException(errStr + ": " + workerIdentityStr);
    }
  }

  /**
   * [NOTE] paired with fromString.
   * if modified, change fromString as well
   * @return String representation of WorkerIdentity
   */
  @Override
  public String toString() {
    return String.format("worker-%s",
        Parsers.getParserOfVersion(mVersion).getVersionSpecificRepresentation(this));
  }

  private void writeObject(java.io.ObjectOutputStream out)
      throws IOException {
    out.defaultWriteObject();
  }

  private void readObject(java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    final Parser parser;
    try {
      parser = Parsers.getParserOfVersion(mVersion);
    } catch (IllegalArgumentException e) {
      throw new InvalidObjectException("Unrecognized identity version: " + mVersion);
    }
    try {
      parser.fromProto(parser.toProto(this));
    } catch (ProtoParsingException e) {
      throw new InvalidObjectException("Validation after deserialization failed: "
          + "data corruption during serialization and/or transmission of the object: " + e);
    }
  }

  /**
   * Parsers for worker identity.
   */
  public static class Parsers {
    /**
     * Parses from a protobuf representation.
     *
     * @param proto proto
     * @return worker identity
     * @throws MissingRequiredFieldsParsingException if required fields are missing
     * @throws InvalidVersionParsingException if the version of the proto message is not valid
     * @throws ProtoParsingException if parsing fails
     */
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
        parser = getParserOfVersion(version);
      } catch (IllegalArgumentException e) {
        throw new InvalidVersionParsingException(version, 0, 1);
      }
      return parser.fromProto(proto);
    }

    /**
     * Converts to a Protobuf representation.
     *
     * @param workerIdentity worker identity
     * @return GRPC representation
     */
    public static alluxio.grpc.WorkerIdentity toProto(WorkerIdentity workerIdentity) {
      return getParserOfVersion(workerIdentity.mVersion).toProto(workerIdentity);
    }

    static Parser getParserOfVersion(int version) throws IllegalArgumentException {
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
     * @return a version-specific representation of worker identity
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

  /**
   * Utility for ser/de with Gson.
   * <p>
   * The version field is encoded as a JSON number, and the identity field
   * is encoded as a hexadecimal string.
   */
  public enum GsonSerde
      implements JsonSerializer<WorkerIdentity>, JsonDeserializer<WorkerIdentity> {
    INSTANCE;

    static final String FIELD_VERSION = "version";
    static final String FIELD_IDENTIFIER = "identifier";

    /**
     * @return serializer
     */
    public JsonSerializer<WorkerIdentity> getSerializer() {
      return WorkerIdentitySerializer.INSTANCE;
    }

    /**
     * @return deserializer
     */
    public JsonDeserializer<WorkerIdentity> getDeserializer() {
      return WorkerIdentityDeserializer.INSTANCE;
    }

    @Override
    public WorkerIdentity deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      return getDeserializer().deserialize(json, typeOfT, context);
    }

    @Override
    public JsonElement serialize(WorkerIdentity src, Type typeOfSrc,
        JsonSerializationContext context) {
      return getSerializer().serialize(src, typeOfSrc, context);
    }
  }

  private enum WorkerIdentitySerializer implements JsonSerializer<WorkerIdentity> {
    INSTANCE;

    @Override
    public JsonElement serialize(WorkerIdentity src, Type typeOfSrc,
        JsonSerializationContext context) {
      JsonObject json = new JsonObject();
      json.addProperty(GsonSerde.FIELD_VERSION, src.mVersion);
      json.addProperty(GsonSerde.FIELD_IDENTIFIER, Hex.encodeHexString(src.mId));
      return json;
    }
  }

  private enum WorkerIdentityDeserializer implements JsonDeserializer<WorkerIdentity> {
    INSTANCE;

    @Override
    public WorkerIdentity deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context)
        throws JsonParseException {
      if (typeOfT != WorkerIdentity.class) {
        throw new JsonParseException("Wrong type for deserialization, "
            + "expecting " + WorkerIdentity.class.getSimpleName() + ", got " + typeOfT);
      }
      if (!json.isJsonObject()) {
        throw new JsonParseException(
            String.format("Expecting worker identity to be an object, got %s",
                json.getClass().getSimpleName()));
      }
      JsonObject object = json.getAsJsonObject();
      alluxio.grpc.WorkerIdentity.Builder protoBuilder = alluxio.grpc.WorkerIdentity.newBuilder();
      JsonElement versionElement = object.get(GsonSerde.FIELD_VERSION);
      if (versionElement != null) {
        int version = parseVersion(versionElement);
        protoBuilder.setVersion(version);
      }

      JsonElement identifierElement = object.get(GsonSerde.FIELD_IDENTIFIER);
      if (identifierElement != null) {
        byte[] identifier = parseIdentifier(identifierElement);
        protoBuilder.setIdentifier(UnsafeByteOperations.unsafeWrap(identifier));
      }

      alluxio.grpc.WorkerIdentity proto = protoBuilder.build();
      try {
        return WorkerIdentity.fromProto(proto);
      } catch (ProtoParsingException e) {
        throw new JsonParseException("Invalid worker identity: " + proto, e);
      }
    }

    private int parseVersion(JsonElement versionElement) throws JsonParseException {
      if (versionElement.isJsonPrimitive()) {
        JsonPrimitive versionPrimitive = versionElement.getAsJsonPrimitive();
        if (versionPrimitive.isNumber()) {
          return versionPrimitive.getAsInt();
        }
      }
      throw new JsonParseException(
          "Expected field " + GsonSerde.FIELD_VERSION + " to be integer, got " + versionElement);
    }

    private byte[] parseIdentifier(JsonElement identifierElement) throws JsonParseException {
      if (identifierElement.isJsonPrimitive()) {
        JsonPrimitive identifierPrimitive = identifierElement.getAsJsonPrimitive();
        if (identifierPrimitive.isString()) {
          try {
            return Hex.decodeHex(identifierPrimitive.getAsString());
          } catch (DecoderException e) {
            throw new JsonParseException(String.format(
                "Invalid hex representation of identifier bytes: %s",
                identifierPrimitive.getAsString()), e);
          }
        }
      }
      throw new JsonParseException("Expected field " + GsonSerde.FIELD_IDENTIFIER
          + " to be string, got " + identifierElement);
    }
  }

  /**
   * JSON serializer for Jackson.
   */
  public static class JacksonSerializer
      extends com.fasterxml.jackson.databind.JsonSerializer<WorkerIdentity> {
    public static final JacksonSerializer INSTANCE = new JacksonSerializer();

    private JacksonSerializer() {
      // prevent instantiation
    }

    @Override
    public void serialize(WorkerIdentity value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      gen.writeNumberField(GsonSerde.FIELD_VERSION, value.mVersion);
      gen.writeStringField(GsonSerde.FIELD_IDENTIFIER, Hex.encodeHexString(value.mId));
      gen.writeEndObject();
    }
  }

  private static class JacksonDeserializeIntermediate {
    @JsonProperty(GsonSerde.FIELD_VERSION)
    @Nullable
    private final String mIdHexEncoded;
    @JsonProperty(GsonSerde.FIELD_IDENTIFIER)
    @Nullable
    private final Integer mVersion;

    @JsonCreator
    public JacksonDeserializeIntermediate(
        @Nullable @JsonProperty(GsonSerde.FIELD_VERSION) Integer version,
        @Nullable @JsonProperty(GsonSerde.FIELD_IDENTIFIER) String hexEncodedIdentifier) {
      mVersion = version;
      mIdHexEncoded = hexEncodedIdentifier;
    }
  }

  /**
   * Deserializer for Jackson.
   */
  public static class JacksonDeserializeConverter
      extends StdConverter<JacksonDeserializeIntermediate, WorkerIdentity> {
    @Override
    public WorkerIdentity convert(JacksonDeserializeIntermediate value) {
      alluxio.grpc.WorkerIdentity.Builder protoBuilder = alluxio.grpc.WorkerIdentity.newBuilder();
      if (value.mIdHexEncoded != null) {
        final byte[] identifier;
        try {
          identifier = Hex.decodeHex(value.mIdHexEncoded);
          protoBuilder
              .setIdentifier(UnsafeByteOperations.unsafeWrap(identifier));
        } catch (DecoderException e) {
          throw new IllegalArgumentException(
              "Malformed hex encoded identifier: " + value.mIdHexEncoded, e);
        }
      }
      if (value.mVersion != null) {
        protoBuilder.setVersion(value.mVersion);
      }
      alluxio.grpc.WorkerIdentity proto = protoBuilder.build();
      try {
        return WorkerIdentity.fromProto(proto);
      } catch (ProtoParsingException e) {
        throw new IllegalArgumentException(
            "Invalid worker identity: " + proto, e);
      }
    }
  }

  /**
   * Funnel to serialize an instance of {@link WorkerIdentity} for hashing.
   */
  @SuppressWarnings("UnstableApiUsage")
  public enum HashFunnel implements Funnel<WorkerIdentity> {
    INSTANCE;

    @Override
    // @Nonnull annotation is needed to work around a bug in spotbugs
    // see https://github.com/spotbugs/spotbugs/pull/2502
    public void funnel(@Nonnull WorkerIdentity from, PrimitiveSink into) {
      into.putBytes(from.mId)
          .putInt(from.mVersion);
    }
  }
}
