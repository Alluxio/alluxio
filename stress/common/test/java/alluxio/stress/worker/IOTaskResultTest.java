package alluxio.stress.worker;

import alluxio.stress.job.IOConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class IOTaskResultTest {
    @Test
    public void json() throws Exception {
        IOTaskResult result = new IOTaskResult();
        result.addPoint(new IOTaskResult.Point(IOConfig.IOMode.READ, 100L, 20));
        result.addPoint(new IOTaskResult.Point(IOConfig.IOMode.WRITE, 100L, 5));
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(result);
        System.out.println(json);
        IOTaskResult other = mapper.readValue(json, IOTaskResult.class);
        // TODO(jiacheng): check equality
        //        checkEquality(config, other);
    }
}
