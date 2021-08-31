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

import alluxio.AlluxioURI;
import alluxio.collections.Pair;
import alluxio.util.JsonSerializable;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.MapType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstract class for parameters of stress tests.
 */
@JsonTypeInfo(use = Id.CLASS, include = As.PROPERTY, property = JsonSerializable.CLASS_NAME_FIELD)
public abstract class Parameters {
  private static final Logger LOG = LoggerFactory.getLogger(Parameters.class);

  /**
   * The shared mapper, which is thread-safe as long as all configuration is complete before any
   * reading and writing.
   */
  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

  private Map<String, Object> toMap() {
    try {
      String json = MAPPER.writeValueAsString(this);
      final MapType type =
          MAPPER.getTypeFactory().constructMapType(Map.class, String.class, Object.class);
      return MAPPER.readValue(json, type);
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
    Set<String> includedFields = new HashSet<>();
    if (fields != null) {
      includedFields.addAll(fields);
    }
    final Map<String, Object> map = toMap();
    List<String> descriptions = new ArrayList<>();

    for (Field field : this.getClass().getFields()) {
      String fieldName = field.getName();
      Object fieldValue = map.get(fieldName);

      if (fields == null || includedFields.contains(fieldName)) {
        // This field description should be added
        if (fieldValue == null) {
          throw new IllegalStateException(String
              .format("Field value is null. fieldName: %s class: %s", fieldName,
                  getClass().getName()));
        }
        String description = null;

        BooleanDescription boolAnnotation = field.getAnnotation(BooleanDescription.class);
        if (boolAnnotation != null) {
          if (!(fieldValue instanceof Boolean)) {
            throw new IllegalStateException(String
                .format("%s must annotate a boolean, but %s is %s", boolAnnotation, field.getName(),
                    field.getType().getName()));
          }
          if ((Boolean) fieldValue) {
            description = boolAnnotation.trueDescription();
          } else {
            description = boolAnnotation.falseDescription();
          }
        }

        PathDescription pathAnnotation = field.getAnnotation(PathDescription.class);
        if (pathAnnotation != null) {
          if (map.containsKey(pathAnnotation.aliasFieldName())) {
            // if alias exists, ignore this field description and only display the alias
            continue;
          } else {
            // show this description path
            AlluxioURI uri = new AlluxioURI(fieldValue.toString());
            description = uri.getScheme();
            if (pathAnnotation.includeAuthority()) {
              description += "-" + uri.getAuthority();
            }
          }
        }

        KeylessDescription keylessAnnotation = field.getAnnotation(KeylessDescription.class);
        if (keylessAnnotation != null) {
          description = fieldValue.toString();
          if (description.isEmpty()) {
            description = "_";
          }
        }

        if (description == null) {
          // default description line
          if (fieldName.startsWith("m")) {
            fieldName = fieldName.substring(1);
          }
          description = String.format("%s: %s", fieldName, fieldValue.toString());
        }
        descriptions.add(description);
      }
    }

    return String.join(", ", descriptions);
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

  /**
   * This annotation defines readable descriptions boolean parameters.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD})
  public @interface BooleanDescription {
    /**
     * @return the description when the boolean value is true
     */
    String trueDescription();

    /**
     * @return the description when the boolean value is false
     */
    String falseDescription();
  }

  /**
   * This annotation defines readable descriptions for paths.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD})
  public @interface PathDescription {
    /**
     * @return the field name that is the alias. If the alias exists, this description is skipped
     */
    String aliasFieldName() default "";

    /**
     * @return true if the authority should be included in the path description
     */
    boolean includeAuthority() default true;
  }

  /**
   * This annotation is for descriptions which do not display the key/name.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD})
  public @interface KeylessDescription {
  }
}
