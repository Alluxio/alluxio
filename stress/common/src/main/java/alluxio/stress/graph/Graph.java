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

package alluxio.stress.graph;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A representation of a graph.
 */
public abstract class Graph {
  /**
   * The shared mapper, which is thread-safe as long as all configuration is complete before any
   * reading and writing.
   */
  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

  protected final Map<String, Object> mGraph = new HashMap<>();
  protected final List<Map<Object, Object>> mData = new ArrayList<>();
  protected final List<String> mTitle = new ArrayList<>();
  protected final List<String> mSubTitle = new ArrayList<>();
  protected final Map<String, List<String>> mErrors = new HashMap<>();

  private Map<String, Object> toMap() {
    mGraph.put("data", ImmutableMap.of("values", mData));
    return mGraph;
  }

  /**
   * @return the list of lines in the title
   */
  public List<String> getTitle() {
    return mTitle;
  }

  /**
   * @param newTitle the list of lines in the title
   */
  public void setTitle(List<String> newTitle) {
    mTitle.clear();
    mTitle.addAll(newTitle);
    mGraph.put("title", ImmutableMap.of("text", mTitle, "subtitle", mSubTitle));
  }

  /**
   * @return the json string representation
   */
  public String toJson() throws JsonProcessingException {
    MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(toMap());
  }

  /**
   * @return map of series name to list of errors
   */
  public Map<String, List<String>> getErrors() {
    return mErrors;
  }

  /**
   * Sets the list of errors for the series.
   *
   * @param series the series name
   * @param errors the list of errors
   */
  public void setErrors(String series, List<String> errors) {
    if (errors.isEmpty()) {
      return;
    }
    mErrors.put(series, errors);
  }
}
