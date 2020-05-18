package alluxio.stress.worker;

import alluxio.stress.job.IOConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        checkEquality(result, other);
    }

    public void checkEquality(IOTaskResult a, IOTaskResult b) {
        assertEquals(a.getPoints().size(), b.getPoints().size());
        Set<IOTaskResult.Point> points = new HashSet<>(a.getPoints());
        System.out.println(points);
        for (IOTaskResult.Point p : b.getPoints()) {
            System.out.println("Verify point "+p);
            assertTrue(points.contains(p));
        }
        assertEquals(a.getErrors().size(), b.getErrors().size());
        Set<String> errors = new HashSet<>(a.getErrors());
        for (String e : b.getErrors()) {
            assertTrue(errors.contains(e));
        }
        return;
    }
}
