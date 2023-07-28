package alluxio.stress.worker;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public class WorkerBenchStatsDeserializer extends JsonDeserializer<WorkerBenchStats> {

    @Override
    public WorkerBenchStats deserialize(JsonParser parser, DeserializationContext ctx)
            throws IOException {
        JsonNode node = parser.getCodec().readTree(parser);
        return new WorkerBenchStats(
                node.get("workerID").asText(), node.get("threadID").asLong(),
                node.get("start").asLong(), node.get("duration").asLong(),
                node.get("ioBytes").asLong()
        );
    }
}