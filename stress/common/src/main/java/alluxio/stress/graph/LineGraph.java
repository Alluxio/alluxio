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
 * A line graph representation.
 */
public class LineGraph extends Graph {
  private static final String X_FIELD = "x";
  private static final String Y_FIELD = "y";
  private static final String SERIES_FIELD = "series";

  /**
   * This represents the data for a line graph.
   */
  public static final class Data {
    private final List<Map<String, Object>> mData;

    /**
     * Creates an instance.
     */
    public Data() {
      mData = new ArrayList<>();
    }

    /**
     * @param x the x value
     * @param y the y value
     */
    public void addData(String x, Object y) {
      mData.add(ImmutableMap.of(LineGraph.X_FIELD, x, LineGraph.Y_FIELD, y));
    }
  }

  /**
   * Creates an instance.
   *
   * @param title the title
   * @param subTitle the lines in the sub title
   * @param xTitle the title of the x-axis
   * @param yTitle the title of the y-axis
   */
  public LineGraph(String title, List<String> subTitle, String xTitle, String yTitle) {
    mTitle.add(title);
    mSubTitle.addAll(subTitle);

    mGraph.put("$schema", "https://vega.github.io/schema/vega-lite/v4.json");
    mGraph.put("width", 700);
    mGraph.put("height", 400);
    mGraph.put("title", ImmutableMap.of("text", mTitle, "subtitle", mSubTitle));

    // TODO(gpang): is this the best we can do to build out this map/json?
    Map<String, Object> encoding = new HashMap<>();
    // TODO(gpang): customize the type
    encoding.put("x", ImmutableMap.of("field", X_FIELD, "type", "ordinal", "title", xTitle));
    encoding.put("y",
        ImmutableMap.of("field", Y_FIELD, "type", "quantitative", "title", yTitle));
    encoding.put("tooltip", Arrays
        .asList(ImmutableMap.of("field", SERIES_FIELD, "type", "nominal", "title", "Series"),
            ImmutableMap.of("field", X_FIELD, "type", "ordinal", "title", xTitle),
            ImmutableMap.of("field", Y_FIELD, "type", "quantitative", "title", yTitle)));
    encoding.put("color", ImmutableMap.of("field", SERIES_FIELD, "type", "nominal"));
    mGraph.put("encoding", encoding);

    List<Object> layer = new ArrayList<>();
    layer.add(ImmutableMap
        .of("mark", ImmutableMap.of("type", "line", "strokeWidth", 16, "stroke", "transparent"),
            "selection", ImmutableMap.of("hover", ImmutableMap
                .of("type", "single", "on", "mouseover", "empty", "none", "fields",
                    new String[] {SERIES_FIELD}))));
    layer.add(ImmutableMap
        .of("encoding", ImmutableMap.of("strokeWidth", ImmutableMap.of("value", 2, "condition",
            ImmutableMap.of("selection", "hover", "value", 6))),
            "mark", ImmutableMap.of("type", "line", "point", ImmutableMap
                .of("filled", false, "fill", "white", "size", 32))));
    mGraph.put("layer", layer);
  }

  /**
   * @param series the series name
   * @param data the series data to add
   */
  public void addDataSeries(String series, LineGraph.Data data) {
    List<Map<String, Object>> newSeries =
        data.mData.stream().map(m -> {
          Map<String, Object> newMap = new HashMap<>(m);
          newMap.put(LineGraph.SERIES_FIELD, series);
          return newMap;
        }).collect(Collectors.toList());
    mData.addAll(newSeries);
  }
}
