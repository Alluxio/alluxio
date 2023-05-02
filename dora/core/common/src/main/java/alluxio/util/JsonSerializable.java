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

package alluxio.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.MapType;

import java.io.IOException;
import java.util.Map;

/**
 * This represents stress test objects that can be ser/de to/from json.
 */
public interface JsonSerializable {
  /** This must match the java field for the class name. */
  String CLASS_NAME_FIELD = "className";

  /**
   * @return the name of the class
   */
  default String getClassName() {
    return this.getClass().getName();
  }

  /**
   * @param className the name of the class
   */
  default void setClassName(String className) {
    // need setter for json ser/de
    // noop, since the class name should not change
  }

  /**
   * @return the json string representation
   */
  default String toJson() throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

    return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  /**
   * @param json the json string representation
   * @return the parsed object
   */
  static JsonSerializable fromJson(String json)
      throws IOException, ClassNotFoundException {
    final ObjectMapper mapper = new ObjectMapper();
    final MapType type =
        mapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class);
    final Map<String, Object> data = mapper.readValue(json, type);

    if (!data.containsKey(CLASS_NAME_FIELD)) {
      throw new IOException("Json data does not contain field: " + CLASS_NAME_FIELD);
    }

    Class<?> clazz = Class.forName(data.get(CLASS_NAME_FIELD).toString());

    return (JsonSerializable) mapper.readValue(json, clazz);
  }

  /**
   * @param json the json string representation
   * @param types the array types to cast the resulting object
   * @param <T> the type to return
   * @return the parsed object
   */
  static <T extends JsonSerializable> T fromJson(String json, T[] types)
      throws IOException, ClassNotFoundException {
    return (T) fromJson(json);
  }
}
