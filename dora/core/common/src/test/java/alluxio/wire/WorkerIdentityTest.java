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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.exception.status.InvalidArgumentException;
import alluxio.util.io.BufferUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.protobuf.ByteString;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class WorkerIdentityTest {
  @Test
  public void legacyIdConvertToLong() {
    WorkerIdentity identity = new WorkerIdentity(Longs.toByteArray(1L), 0);
    assertEquals(1L, WorkerIdentity.ParserV0.INSTANCE.toLong(identity));
    assertEquals(1L, WorkerIdentity.ParserV0.INSTANCE.getVersionSpecificRepresentation(identity));
  }

  @Test
  public void legacyIdConvertFromLong() {
    WorkerIdentity identity = WorkerIdentity.ParserV0.INSTANCE.fromLong(1L);
    assertEquals(new WorkerIdentity(Longs.toByteArray(1L), 0), identity);
  }

  @Test
  public void legacyConvertToProto() throws Exception {
    WorkerIdentity identity = new WorkerIdentity(Longs.toByteArray(1L), 0);
    alluxio.grpc.WorkerIdentity identityProto = identity.toProto();
    assertEquals(alluxio.grpc.WorkerIdentity.newBuilder()
            .setVersion(0)
            .setIdentifier(ByteString.copyFrom(Longs.toByteArray(1L)))
            .build(),
        identityProto);
  }

  @Test
  public void legacyConvertFromProto() throws Exception {
    alluxio.grpc.WorkerIdentity identityProto = alluxio.grpc.WorkerIdentity.newBuilder()
        .setVersion(0)
        .setIdentifier(ByteString.copyFrom(Longs.toByteArray(2L)))
        .build();
    WorkerIdentity identity = new WorkerIdentity(Longs.toByteArray(2L), 0);
    assertEquals(identity, WorkerIdentity.fromProto(identityProto));
  }

  @Test
  public void legacyVersionMismatch() {
    alluxio.grpc.WorkerIdentity identityProto = alluxio.grpc.WorkerIdentity.newBuilder()
        .setVersion(1)
        .setIdentifier(ByteString.copyFrom(Longs.toByteArray(2L)))
        .build();
    assertThrows(InvalidVersionParsingException.class,
        () -> WorkerIdentity.ParserV0.INSTANCE.fromProto(identityProto));
  }

  @Test
  public void legacyInvalidIdentifier() {
    alluxio.grpc.WorkerIdentity identityProto = alluxio.grpc.WorkerIdentity.newBuilder()
        .setVersion(0)
        .setIdentifier(ByteString.copyFrom("a byte string longer than 8 bytes".getBytes()))
        .build();
    assertThrows(ProtoParsingException.class,
        () -> WorkerIdentity.ParserV0.INSTANCE.fromProto(identityProto));
  }

  @Test
  public void legacyMissingFields() {
    alluxio.grpc.WorkerIdentity missingId = alluxio.grpc.WorkerIdentity.newBuilder()
        .setVersion(1)
        .build();
    assertThrows(MissingRequiredFieldsParsingException.class,
        () -> WorkerIdentity.ParserV1.INSTANCE.fromProto(missingId));

    alluxio.grpc.WorkerIdentity missingVersion = alluxio.grpc.WorkerIdentity.newBuilder()
        .setIdentifier(ByteString.copyFrom("id", Charset.defaultCharset()))
        .build();
    assertThrows(MissingRequiredFieldsParsingException.class,
        () -> WorkerIdentity.ParserV0.INSTANCE.fromProto(missingVersion));
  }

  @Test
  public void v1ConvertFromUuid() {
    UUID uuid = UUID.randomUUID();
    WorkerIdentity identity = WorkerIdentity.ParserV1.INSTANCE.fromUUID(uuid);
    alluxio.grpc.WorkerIdentity proto = identity.toProto();
    ByteBuffer uuidBytes = proto.getIdentifier().asReadOnlyByteBuffer();
    assertEquals(uuid, new UUID(uuidBytes.getLong(), uuidBytes.getLong()));

    identity = WorkerIdentity.ParserV1.INSTANCE.fromUUID(uuid.toString());
    proto = identity.toProto();
    uuidBytes = proto.getIdentifier().asReadOnlyByteBuffer();
    assertEquals(uuid, new UUID(uuidBytes.getLong(), uuidBytes.getLong()));
  }

  @Test
  public void v1ConvertToUuid() {
    byte[] uuidBytes = BufferUtils.getIncreasingByteArray(16);
    ByteBuffer buffer = ByteBuffer.wrap(uuidBytes);
    UUID uuid = new UUID(buffer.getLong(), buffer.getLong());
    WorkerIdentity identity = new WorkerIdentity(uuidBytes, 1);
    assertEquals(uuid, WorkerIdentity.ParserV1.INSTANCE.toUUID(identity));
  }

  @Test
  public void v1InvalidIdentifier() {
    alluxio.grpc.WorkerIdentity identityProto = alluxio.grpc.WorkerIdentity.newBuilder()
        .setVersion(1)
        .setIdentifier(ByteString.copyFrom("non-uuid".getBytes(StandardCharsets.UTF_8)))
        .build();
    assertThrows(ProtoParsingException.class,
        () -> WorkerIdentity.ParserV1.INSTANCE.fromProto(identityProto));
  }

  @Test
  public void parserFromProto() throws Exception {
    WorkerIdentity identity = WorkerIdentity.Parsers.fromProto(
        alluxio.grpc.WorkerIdentity.newBuilder()
            .setVersion(0)
            .setIdentifier(ByteString.copyFrom(Longs.toByteArray(1L)))
            .build());
    assertEquals(new WorkerIdentity(Longs.toByteArray(1L), 0), identity);
  }

  @Test
  public void parserToProto() throws Exception {
    byte[] idBytes = BufferUtils.getIncreasingByteArray(16);
    WorkerIdentity identity = new WorkerIdentity(idBytes, 1);
    alluxio.grpc.WorkerIdentity proto = WorkerIdentity.Parsers.toProto(identity);
    assertEquals(1, proto.getVersion());
    assertArrayEquals(idBytes, proto.getIdentifier().toByteArray());
  }

  @Test
  public void parserInvalidVersion() throws Exception {
    alluxio.grpc.WorkerIdentity proto = alluxio.grpc.WorkerIdentity.newBuilder()
        .setVersion(-1)
        .setIdentifier(ByteString.copyFrom(Longs.toByteArray(1L)))
        .build();
    assertThrows(InvalidVersionParsingException.class,
        () -> WorkerIdentity.Parsers.fromProto(proto));
  }

  @Test
  public void testEquals() {
    WorkerIdentity identity1 = new WorkerIdentity(new byte[]{0x1}, 2);
    WorkerIdentity identity2 = new WorkerIdentity(new byte[]{0x1}, 2);
    assertEquals(identity1, identity2);
  }

  @Test
  public void gsonSerialization() {
    long id = RandomUtils.nextLong();
    byte[] idBytes = Longs.toByteArray(id);
    WorkerIdentity identity = new WorkerIdentity(idBytes, 0);
    Gson gson = new GsonBuilder()
        .registerTypeAdapter(WorkerIdentity.class, WorkerIdentity.GsonSerde.INSTANCE)
        .create();
    assertEquals("{\"version\":0,\"identifier\":\"" + Hex.encodeHexString(idBytes) + "\"}",
        gson.toJson(identity));
  }

  @Test
  public void gsonDeserialization() {
    byte[] uuidBytes = BufferUtils.getIncreasingByteArray(16);
    String jsonString = "{\"version\":1,\"identifier\":\"" + Hex.encodeHexString(uuidBytes) + "\"}";
    Gson gson = new GsonBuilder()
        .registerTypeAdapter(WorkerIdentity.class, WorkerIdentity.GsonSerde.INSTANCE)
        .create();
    WorkerIdentity identity = gson.fromJson(jsonString, WorkerIdentity.class);

    assertEquals(new WorkerIdentity(uuidBytes, 1), identity);
  }

  @Test
  public void gsonDeserializationWithFieldsMissing() {
    Gson gson = new GsonBuilder()
        .registerTypeAdapter(WorkerIdentity.class, WorkerIdentity.GsonSerde.INSTANCE)
        .create();
    String missingId = "{\"version\":1}";
    Throwable jsonParseException =
        assertThrows(JsonParseException.class,
            () -> gson.fromJson(missingId, WorkerIdentity.class));
    assertTrue(jsonParseException.getCause() instanceof MissingRequiredFieldsParsingException);

    String missingVersion = "{\"identifier\":\"0badbeef\"}";
    jsonParseException =
        assertThrows(JsonParseException.class,
            () -> gson.fromJson(missingVersion, WorkerIdentity.class));
    assertTrue(jsonParseException.getCause() instanceof MissingRequiredFieldsParsingException);
  }

  @Test
  public void jacksonSerialization() throws Exception {
    long id = RandomUtils.nextLong();
    byte[] idBytes = Longs.toByteArray(id);
    WorkerIdentity identity = new WorkerIdentity(idBytes, 0);
    ObjectMapper objectMapper = new ObjectMapper();
    assertEquals("{\"version\":0,\"identifier\":\"" + Hex.encodeHexString(idBytes) + "\"}",
        objectMapper.writeValueAsString(identity));
  }

  @Test
  public void jacksonDeserialization() throws Exception {
    byte[] uuidBytes = BufferUtils.getIncreasingByteArray(16);
    String jsonString = "{\"version\":1,\"identifier\":\"" + Hex.encodeHexString(uuidBytes) + "\"}";
    ObjectMapper objectMapper = new ObjectMapper();
    WorkerIdentity identity = objectMapper.readValue(jsonString, WorkerIdentity.class);

    assertEquals(new WorkerIdentity(uuidBytes, 1), identity);
  }

  @Test
  public void jacksonDeserializationWithFieldsMissing() {
    ObjectMapper mapper = new ObjectMapper();
    String missingId = "{\"version\":1}";
    Throwable jsonParseException =
        assertThrows(IllegalArgumentException.class,
            () -> mapper.readValue(missingId, WorkerIdentity.class));
    assertTrue(jsonParseException.getCause() instanceof MissingRequiredFieldsParsingException);

    String missingVersion = "{\"identifier\":\"0badbeef\"}";
    jsonParseException =
        assertThrows(IllegalArgumentException.class,
            () -> mapper.readValue(missingVersion, WorkerIdentity.class));
    assertTrue(jsonParseException.getCause() instanceof MissingRequiredFieldsParsingException);
  }

  @Test
  public void javaSerde() throws Exception {
    UUID uuid = UUID.nameUUIDFromBytes("uuid".getBytes(StandardCharsets.UTF_8));
    WorkerIdentity identity = WorkerIdentity.ParserV1.INSTANCE.fromUUID(uuid);
    final byte[] buffer;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(identity);
    }
    buffer = bos.toByteArray();
    try (ByteArrayInputStream bis = new ByteArrayInputStream(buffer);
         ObjectInputStream ois = new ObjectInputStream(bis)) {
      Object obj = ois.readObject();
      assertTrue(obj instanceof WorkerIdentity);
      WorkerIdentity idDeserialized = (WorkerIdentity) obj;
      assertEquals(identity, idDeserialized);
    }
  }

  @Test
  public void javaSerdeInvalidVersion() throws Exception {
    int invalidVersion = -1;
    byte[] data = new byte[]{0x1, 0x2, 0x3, 0x4};
    WorkerIdentity identity = new WorkerIdentity(data, invalidVersion);
    final byte[] buffer;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(identity);
    }
    buffer = bos.toByteArray();
    try (ByteArrayInputStream bis = new ByteArrayInputStream(buffer);
         ObjectInputStream ois = new ObjectInputStream(bis)) {
      InvalidObjectException t = assertThrows(InvalidObjectException.class,
          ois::readObject);
      t.printStackTrace();
    }
  }

  @Test
  public void javaSerdeInvalidData() throws Exception {
    int version0 = 0;
    byte[] invalidIdForVersion0 = new byte[]{0x1, 0x2, 0x3, 0x4};
    WorkerIdentity identity = new WorkerIdentity(invalidIdForVersion0, version0);
    final byte[] buffer;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(identity);
    }
    buffer = bos.toByteArray();
    try (ByteArrayInputStream bis = new ByteArrayInputStream(buffer);
         ObjectInputStream ois = new ObjectInputStream(bis)) {
      InvalidObjectException t = assertThrows(InvalidObjectException.class,
          ois::readObject);
      t.printStackTrace();
    }
  }

  @Test
  public void stringSerDeTest() throws InvalidArgumentException {
    // V1 string SerDe
    UUID uuid = UUID.nameUUIDFromBytes("uuid".getBytes(StandardCharsets.UTF_8));
    WorkerIdentity identity = WorkerIdentity.ParserV1.INSTANCE.fromUUID(uuid);
    String idStr = identity.toString();
    WorkerIdentity deserializedId = WorkerIdentity.fromString(idStr);
    Assert.assertEquals(identity, deserializedId);

    // V0 string SerDe
    Long longId = RandomUtils.nextLong();
    identity = WorkerIdentity.ParserV0.INSTANCE.fromLong(longId);
    idStr = identity.toString();
    deserializedId =  WorkerIdentity.fromString(idStr);
    Assert.assertEquals(identity, deserializedId);

    // Deserialize an unrecognized id str should get a InvalidArgumentException exception
    String invalidIdStr = String.format("{}-{}",
        RandomUtils.nextLong(), RandomUtils.nextLong());
    Assert.assertThrows(InvalidArgumentException.class,
        () -> WorkerIdentity.fromString(invalidIdStr));
  }
}
