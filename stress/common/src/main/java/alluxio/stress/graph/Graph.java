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
  protected final Map<String, Object> mGraph = new HashMap<>();
  protected final List<Map<String, Object>> mData = new ArrayList<>();
  protected final List<String> mTitle = new ArrayList<>();
  protected final List<String> mSubTitle = new ArrayList<>();

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
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(toMap());
  }
}
