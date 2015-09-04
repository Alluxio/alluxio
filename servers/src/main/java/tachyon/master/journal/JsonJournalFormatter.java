/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.journal;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import tachyon.TachyonURI;
import tachyon.master.block.journal.BlockIdGeneratorEntry;
import tachyon.master.block.journal.BlockInfoEntry;
import tachyon.master.block.journal.WorkerIdGeneratorEntry;
import tachyon.master.file.journal.AddCheckpointEntry;
import tachyon.master.file.journal.CompleteFileEntry;
import tachyon.master.file.journal.DeleteFileEntry;
import tachyon.master.file.journal.DependencyEntry;
import tachyon.master.file.journal.FreeEntry;
import tachyon.master.file.journal.InodeDirectoryEntry;
import tachyon.master.file.journal.InodeDirectoryIdGeneratorEntry;
import tachyon.master.file.journal.InodeFileEntry;
import tachyon.master.file.journal.RenameEntry;
import tachyon.master.file.journal.SetPinnedEntry;
import tachyon.master.file.meta.DependencyType;
import tachyon.master.rawtable.journal.RawTableEntry;
import tachyon.master.user.journal.UserIdGeneratorEntry;

public final class JsonJournalFormatter implements JournalFormatter {
  private static class JsonEntry {
    /** Creates a JSON ObjectMapper configured not to close the underlying stream. */
    public static ObjectMapper createObjectMapper() {
      // TODO: Could disable field name quoting, though this would produce technically invalid JSON
      // See: JsonGenerator.QUOTE_FIELD_NAMES and JsonParser.ALLOW_UNQUOTED_FIELD_NAMES
      return new ObjectMapper().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)
          .configure(SerializationFeature.CLOSE_CLOSEABLE, false);
    }

    public static final ObjectMapper OBJECT_MAPPER = createObjectMapper();
    public static final ObjectWriter OBJECT_WRITER = OBJECT_MAPPER.writer();

    public final long mSequenceNumber;
    public final JournalEntryType mType;
    public Map<String, JsonNode> mParameters = Maps.newHashMap();

    public JsonEntry(long SequenceNumber, JournalEntryType type) {
      mSequenceNumber = SequenceNumber;
      mType = type;
    }

    /** Constructor used for deserializing the entry. */
    @JsonCreator
    public JsonEntry(@JsonProperty("sequenceNumber") long SequenceNumber,
        @JsonProperty("type") JournalEntryType type,
        @JsonProperty("parameters") Map<String, JsonNode> parameters) {
      mSequenceNumber = SequenceNumber;
      mType = type;
      mParameters = parameters;
    }

    /**
     * Generic parameter getter, useful for custom classes or enums. Use a more specific getter,
     * like getLong(), when available.
     */
    public <T> T get(String name, Class<T> clazz) {
      return OBJECT_MAPPER.convertValue(mParameters.get(name), clazz);
    }

    /**
     * Get the value for parameterized type class such as {@code List<Integer>} using the help of
     * <code>TypeReference</code>
     *
     * @param name the name of the parameter
     * @param typeReference the type class of value
     * @return the value in its type class
     */
    public <T> T get(String name, TypeReference<T> typeReference) {
      return OBJECT_MAPPER.convertValue(mParameters.get(name), typeReference);
    }

    /**
     * Deserializes a parameter as {@code Boolean}.
     *
     * @param name the name of the parameter
     * @return deserialized value of this parameter in {@code Boolean}
     */
    public Boolean getBoolean(String name) {
      return this.get(name, Boolean.class);
    }

    /**
     * Deserializes a parameter as {@code ByteBuffer} which is stored as a base64-encoded String.
     *
     * @param name the name of the parameter
     * @return deserialized value of this parameter in {@code ByteBuffer}
     */
    public ByteBuffer getByteBuffer(String name) {
      String byteString = get(name, String.class);
      if (byteString == null) {
        return null;
      }

      return ByteBuffer.wrap(Base64.decodeBase64(byteString));
    }

    /**
     * Deserializes a parameter as {@code List<ByteBuffer>} which is stored as a list of
     * base64-encoded {@code String}s.
     *
     * @param name the name of the parameter
     * @return deserialized value of this parameter in {@code List<ByteBuffer>}
     */
    public List<ByteBuffer> getByteBufferList(String name) {
      List<String> byteStrings = get(name, new TypeReference<List<String>>() {});
      if (byteStrings == null) {
        return null;
      }

      List<ByteBuffer> buffers = Lists.newArrayListWithCapacity(byteStrings.size());
      for (String byteString : byteStrings) {
        buffers.add(ByteBuffer.wrap(Base64.decodeBase64(byteString)));
      }
      return buffers;
    }

    /**
     * Deserializes a parameter as {@code Integer}.
     *
     * @param name the name of the parameter
     * @return deserialized value of this parameter in {@code Integer}
     */
    public Integer getInt(String name) {
      return this.get(name, Number.class).intValue();
    }

    /**
     * Deserializes a parameter as {@code Long}. Use of this function is necessary when dealing with
     * longs, as they may have been deserialized as integers if they were sufficiently small.
     *
     * @param name the name of the parameter
     * @return deserialized value of this parameter in {@code Long}
     */
    public Long getLong(String name) {
      return this.get(name, Number.class).longValue();
    }

    /**
     * Deserializes a parameter as {@code String}.
     *
     * @param name the name of the parameter
     * @return deserialized value of this parameter in {@code String}
     */
    public String getString(String name) {
      return this.get(name, String.class);
    }

    /**
     * Adds the given named parameter to the Json object. Value must be JSON-serializable.
     *
     * @param name the name of the parameter
     * @param value the value of the parameter
     * @return this Json object
     */
    public JsonEntry withParameter(String name, Object value) {
      mParameters.put(name, OBJECT_MAPPER.convertValue(value, JsonNode.class));
      return this;
    }
  }

  @Override
  public void serialize(SerializableJournalEntry entry, OutputStream outputStream)
      throws IOException {
    // serialize parameters
    Map<String, Object> parameters = entry.getParameters();
    JsonEntry jsonEntry = new JsonEntry(entry.getSequenceNumber(), entry.getType());
    for (Map.Entry<String, Object> parameter : parameters.entrySet()) {
      jsonEntry.withParameter(parameter.getKey(), parameter.getValue());
    }
    writeEntry(jsonEntry, outputStream);
  }

  private void writeEntry(JsonEntry entry, OutputStream os) throws IOException {
    JsonEntry.OBJECT_WRITER.writeValue(os, entry);
    (new DataOutputStream(os)).write('\n');
  }

  @Override
  public JournalInputStream deserialize(final InputStream inputStream) throws IOException {
    return new JournalInputStream() {
      private JsonParser mParser = JsonEntry.createObjectMapper().getFactory()
          .createParser(inputStream);

      @Override
      public JournalEntry getNextEntry() throws IOException {
        JsonEntry entry;
        try {
          entry = mParser.readValueAs(JsonEntry.class);
        } catch (JsonProcessingException e) {
          return null;
        }
        switch (entry.mType) {
          // User
          case USER_ID_GENERATOR: {
            return new UserIdGeneratorEntry(
                entry.getLong("nextUserId"));
          }

          // Block
          case BLOCK_ID_GENERATOR: {
            return new BlockIdGeneratorEntry(
                entry.getLong("nextContainerId"));
          }
          case BLOCK_INFO: {
            return new BlockInfoEntry(
                entry.getLong("blockId"),
                entry.getLong("length"));
          }
          case WORKER_ID_GENERATOR: {
            return new WorkerIdGeneratorEntry(
                entry.getLong("nextWorkerId"));
          }

          // FileSystem
          case INODE_FILE: {
            return new InodeFileEntry(
                entry.getLong("creationTimeMs"),
                entry.getLong("id"),
                entry.getString("name"),
                entry.getLong("parentId"),
                entry.getBoolean("isPinned"),
                entry.getLong("lastModificationTimeMs"),
                entry.getLong("blockSizeBytes"),
                entry.getLong("length"),
                entry.getBoolean("isComplete"),
                entry.getBoolean("isCache"),
                entry.getString("ufsPath"),
                entry.get("blocks", new TypeReference<List<Long>>() {}));
          }
          case INODE_DIRECTORY: {
            return new InodeDirectoryEntry(
                entry.getLong("creationTimeMs"),
                entry.getLong("id"),
                entry.getString("name"),
                entry.getLong("parentId"),
                entry.getBoolean("isPinned"),
                entry.getLong("lastModificationTimeMs"),
                entry.get("childrenIds", new TypeReference<Set<Long>>() {}));
          }
          case ADD_CHECKPOINT: {
            return new AddCheckpointEntry(
                entry.getLong("workerId"),
                entry.getLong("fileId"),
                entry.getLong("length"),
                new TachyonURI(entry.getString("checkpointPath")),
                entry.getLong("operationTimeMs"));
          }
          case DEPENDENCY: {
            return new DependencyEntry(
                entry.getInt("id"),
                entry.get("parentFiles", new TypeReference<List<Long>>() {
                }),
                entry.get("childrenFiles", new TypeReference<List<Long>>() {
                }),
                entry.getString("commandPrefix"),
                entry.getByteBufferList("data"),
                entry.getString("comment"),
                entry.getString("framework"),
                entry.getString("frameworkVersion"),
                entry.get("dependencyType", DependencyType.class),
                entry.get("parentDependencies", new TypeReference<List<Integer>>() {
                }),
                entry.get("childrenDependencies", new TypeReference<List<Integer>>() {
                }),
                entry.getLong("creationTimeMs"),
                entry.get("uncheckpointedFiles", new TypeReference<List<Long>>() {
                }),
                entry.get("lostFileIds", new TypeReference<Set<Long>>() {
                }));
          }
          case COMPLETE_FILE: {
            return new CompleteFileEntry(
                entry.get("blockIds", new TypeReference<List<Long>>() {
                }),
                entry.getLong("id"),
                entry.getLong("length"),
                entry.getLong("operationTimeMs"));
          }
          case FREE: {
            return new FreeEntry(
                entry.getLong("id"));
          }
          case SET_PINNED: {
            return new SetPinnedEntry(
                entry.getLong("id"),
                entry.getBoolean("pinned"),
                entry.getLong("operationTimeMs"));
          }
          case DELETE_FILE: {
            return new DeleteFileEntry(
                entry.getLong("fileId"),
                entry.getBoolean("recursive"),
                entry.getLong("operationTimeMs"));
          }
          case RENAME: {
            return new RenameEntry(
                entry.getLong("fileId"),
                entry.getString("destinationPath"),
                entry.getLong("operationTimeMs"));
          }
          case INODE_DIRECTORY_ID_GENERATOR: {
            return new InodeDirectoryIdGeneratorEntry(
                entry.getLong("containerId"),
                entry.getLong("sequenceNumber"));
          }

          // RawTable
          case RAW_TABLE: {
            return new RawTableEntry(
                entry.getLong("id"),
                entry.getInt("columns"),
                entry.getByteBuffer("metadata"));
          }
          default:
            throw new IOException("Unknown entry type: " + entry.mType);
        }
      }

      @Override
      public void close() throws IOException {
        inputStream.close();
      }
    };
  }
}
