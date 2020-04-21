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

package alluxio.stress;

import alluxio.collections.Pair;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.MapType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstract class for parameters of stress tests.
 */
public abstract class Parameters {
  protected Map<String, Object> toMap() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    try {
      String json = mapper.writeValueAsString(this);
      final MapType type =
          mapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class);
      return mapper.readValue(json, type);
    } catch (Exception e) {
      throw new RuntimeException("Failed to convert " + this.getClass().getName() + " to map.", e);
    }
  }

  /**
   * Returns a string representation of the parameters, using only the specified fields.
   *
   * @param fields the fields to use for the string representation
   * @return the string representation
   */
  public String getDescription(List<String> fields) {
    final Map<String, Object> map = toMap();
    return fields.stream().map(f -> f + ": " + map.get(f)).collect(Collectors.joining(", "));
  }

  /**
   * @param paramList the list of parameters to extract the common and unique fields
   * @return a Pair of list of field names, (common fields, unique fields)
   */
  public static Pair<List<String>, List<String>> partitionFieldNames(List<Parameters> paramList) {
    List<Map<String, Object>> paramMaps =
        paramList.stream().map(Parameters::toMap).collect(Collectors.toList());

    Map<String, Object> constantValues = paramMaps.get(0);
    Set<String> uniqueFields = new HashSet<>();

    for (Map<String, Object> param : paramMaps) {
      Set<String> rm = new HashSet<>(constantValues.keySet());
      rm.removeAll(param.keySet());
      uniqueFields.addAll(rm);

      for (Map.Entry<String, Object> entry : param.entrySet()) {
        if (uniqueFields.contains(entry.getKey())) {
          continue;
        }
        if (!entry.getValue().equals(constantValues.get(entry.getKey()))) {
          uniqueFields.add(entry.getKey());
        }
      }
    }

    Set<String> commonFields = new HashSet<>(constantValues.keySet());
    commonFields.removeAll(uniqueFields);

    // TODO(gpang): special handling for map values

    return new Pair<>(new ArrayList<>(commonFields), new ArrayList<>(uniqueFields));
  }
}
