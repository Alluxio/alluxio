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

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A bar graph representation.
 */
public class BarGraph extends Graph {
  private static final String VALUE_FIELD = "y";
  private static final String SERIES_FIELD = "series";

  /**
   * Class representing data for a bar graph.
   */
  public static class Data {
    private final List<Map<Object, Object>> mData;

    /**
     * Creates an instance.
     */
    public Data() {
      mData = new ArrayList<>();
    }

    /**
     * @param value the value
     */
    public void addData(Object value) {
      mData.add(ImmutableMap.of(BarGraph.VALUE_FIELD, value));
    }
  }

  /**
   * Creates an instance.
   *
   * @param title the title
   * @param subTitle the lines in the sub title
   * @param xTitle the title of the x-axis
   */
  public BarGraph(String title, List<String> subTitle, String xTitle) {
    mTitle.add(title);
    mSubTitle.addAll(subTitle);

    mGraph.put("$schema", "https://vega.github.io/schema/vega-lite/v4.json");
    mGraph.put("width", 700);
    mGraph.put("height", 400);
    mGraph.put("title", ImmutableMap.of("text", mTitle, "subtitle", mSubTitle));
    mGraph.put("mark", "bar");

    // TODO(gpang): is this the best we can do to build out this map/json?
    Map<String, Object> encoding = new HashMap<>();
    // TODO(gpang): customize the type
    encoding
        .put("x", ImmutableMap.of("field", VALUE_FIELD, "type", "quantitative", "title", xTitle));
    encoding.put("y",
        ImmutableMap.of("field", SERIES_FIELD, "type", "nominal", "title", ""));
    encoding.put("tooltip", Arrays
        .asList(ImmutableMap.of("field", SERIES_FIELD, "type", "nominal", "title", "Series"),
            ImmutableMap.of("field", VALUE_FIELD, "type", "quantitative", "title", xTitle)));
    mGraph.put("encoding", encoding);
  }

  /**
   * @param series the series name
   * @param data the series data to add
   */
  public void addDataSeries(String series, Data data) {
    List<Map<Object, Object>> newSeries =
        data.mData.stream().map(m -> {
          Map<Object, Object> newMap = new HashMap<>(m);
          newMap.put(BarGraph.SERIES_FIELD, series);
          return newMap;
        }).collect(Collectors.toList());
    mData.addAll(newSeries);
  }
}
