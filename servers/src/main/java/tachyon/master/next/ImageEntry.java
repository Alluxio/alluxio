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

package tachyon.master.next;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Objects;

/**
 * Each entry in the Image is represented as a single element, which is serialized as JSON. An
 * element has a type and a set of parameters determined by the type.
 */
public class ImageEntry extends JsonObject {
  private ImageEntryType mType;

  /** Create a new ImageEntry with internal entry type initialized */
  public ImageEntry(ImageEntryType type) {
    mType = type;
  }

  /** Constructor used for deserializing ImageEntry. */
  @JsonCreator
  public ImageEntry(@JsonProperty("type") ImageEntryType type,
      @JsonProperty("parameters") Map<String, JsonNode> parameters) {
    mType = type;
    mParameters = parameters;
  }

  /** Type of this ImageEntry */
  public ImageEntryType type() {
    return mType;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("type", mType).add("parameters", mParameters)
        .toString();
  }

  @Override
  public void dump(ObjectWriter objWriter, DataOutputStream dos) throws IOException {
    objWriter.writeValue(dos, this);
    dos.writeByte('\n');
  }

  @Override
  public ImageEntry withParameter(String name, Object value) {
    return (ImageEntry) super.withParameter(name, value);
  }
}
