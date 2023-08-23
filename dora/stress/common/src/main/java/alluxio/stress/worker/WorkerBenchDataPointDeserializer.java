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

import java.io.IOException;

/**
 * A deserializer converting {@link WorkerBenchDataPoint} from JSON.
 */
public class WorkerBenchDataPointDeserializer extends JsonDeserializer<WorkerBenchDataPoint> {
  @Override
  public WorkerBenchDataPoint deserialize(JsonParser parser, DeserializationContext ctx)
          throws IOException {
    JsonNode node = parser.getCodec().readTree(parser);
    return new WorkerBenchDataPoint(
            node.get("workerID").asText(), node.get("threadID").asLong(),
            node.get("startMs").asLong(), node.get("duration").asLong(),
            node.get("ioBytes").asLong()
    );
  }
}
