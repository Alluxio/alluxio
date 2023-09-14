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
    Long wId = node.get("wid").asLong();
    Long tId = node.get("tid").asLong();
    List<Long> tpList = new ArrayList<>();
    JsonNode tpNode = node.get("throughput");
    if (tpNode != null) {
      for (JsonNode throughput : tpNode) {
        tpList.add(throughput.asLong());
      }
    }
    List<List<WorkerBenchDataPoint>> data = new ArrayList<>();
    JsonNode dataNode = node.get("data");
    if (dataNode != null) {
      for (JsonNode listNode: dataNode) {
        List<WorkerBenchDataPoint> dataPoints = new ArrayList<>();
        for (JsonNode subListNode: listNode) {
          WorkerBenchDataPoint dataPoint = mapper
              .treeToValue(subListNode, WorkerBenchDataPoint.class);
          dataPoints.add(dataPoint);
        }
        data.add(dataPoints);
      }
    }
    return new WorkerBenchCoarseDataPoint(wId, tId, data, tpList);
  }
}
