package alluxio.stress.worker;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

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
