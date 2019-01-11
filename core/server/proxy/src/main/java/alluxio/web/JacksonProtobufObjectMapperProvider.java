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

package alluxio.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

/**
 * Custom context resolver for Jersey to handle JSON-to-Protobuf conversion.
 */
@Provider
public class JacksonProtobufObjectMapperProvider implements ContextResolver<ObjectMapper> {
  final ObjectMapper mDefaultObjectMapper;

  /**
   * Create {@link ObjectMapper} provider that handles protobuf.
   */
  public JacksonProtobufObjectMapperProvider() {
    mDefaultObjectMapper = createDefaultMapper();
  }

  private static ObjectMapper createDefaultMapper() {
    final ObjectMapper jackson = new ObjectMapper();
    jackson.setPropertyNamingStrategy(PropertyNamingStrategy.LOWER_CAMEL_CASE);
    jackson.registerModule(new ProtobufModule());
    return jackson;
  }

  @Override
  public ObjectMapper getContext(Class<?> type) {
    return mDefaultObjectMapper;
  }
}
