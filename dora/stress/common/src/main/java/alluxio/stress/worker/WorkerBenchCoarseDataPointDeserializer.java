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

package alluxio.stress.worker;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A deserializer converting {@link WorkerBenchCoarseDataPoint} from JSON.
 */
public class WorkerBenchCoarseDataPointDeserializer
    extends JsonDeserializer<WorkerBenchCoarseDataPoint> {
  @Override
  public WorkerBenchCoarseDataPoint deserialize(JsonParser parser, DeserializationContext ctx)
      throws IOException {
    ObjectMapper mapper = (ObjectMapper) parser.getCodec();
    JsonNode node = parser.getCodec().readTree(parser);
    Long wId = node.get("workerId").asLong();
    Long tId = node.get("threadId").asLong();
    List<Long> tpList = new ArrayList<>();
    JsonNode tpNode = node.get("throughput");
    if (tpNode != null) {
      for (JsonNode throughput : tpNode) {
        tpList.add(throughput.asLong());
      }
    }
    List<WorkerBenchDataPoint> data = new ArrayList<>();
    JsonNode dataNodes = node.get("data");
    if (dataNodes != null) {
      for (JsonNode dataNode: dataNodes) {
        WorkerBenchDataPoint dataPoint = mapper
            .treeToValue(dataNode, WorkerBenchDataPoint.class);
        data.add(dataPoint);
      }
    }
    return new WorkerBenchCoarseDataPoint(wId, tId, data, tpList);
  }
}
