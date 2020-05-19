package alluxio.stress.job;

import alluxio.job.plan.replicate.EvictConfig;
import alluxio.stress.worker.WorkerBenchParameters;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class IOConfigTest {
    @Test
    public void json() throws Exception {
        IOConfig config = new IOConfig(IOConfig.class.getCanonicalName(),
                ImmutableList.of(),
        16,
        1024,
        1,
        "hdfs://namenode:9000/alluxio");
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(config);
        System.out.println(json);
        IOConfig other = mapper.readValue(json, IOConfig.class);
        // TODO(jiacheng): check equality
        //        checkEquality(config, other);
    }
}
