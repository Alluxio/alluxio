package tachyon.master;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Unit Test for EditLogOperation
 */
public class EditLogOperationTest {
  private static final String CREATE_DEPENDENCY_TYPE = "{\"type\":\"CREATE_DEPENDENCY\"," +
      "\"parameters\":{\"parents\":[],\"commandPrefix\":\"fake command\"," +
      "\"dependencyId\":1,\"frameworkVersion\":\"0.3\",\"data\":[\"AAAAAAAAAAAAAA==\"]," +
      "\"children\":[251,252,253,254,255,256,257,258,259,260]," +
      "\"comment\":\"BasicCheckpoint Dependency\",\"creationTimeMs\":1409349750338," +
      "\"dependencyType\":\"Narrow\",\"framework\":\"Tachyon Examples\"}}";

  private static final ObjectMapper OBJECT_MAPPER = JsonObject.createObjectMapper();

  // Tests for CREATE_DEPENDENCY operation
  @Test
  public void createDependencyTest() throws IOException {
    EditLogOperation editLogOperation = OBJECT_MAPPER.readValue(CREATE_DEPENDENCY_TYPE.getBytes(),
        EditLogOperation.class);

    // get all parameters for "CREATE_DEPENDENCY"
    editLogOperation.get("parents", new TypeReference<List<Integer>>() {});
    editLogOperation.get("children", new TypeReference<List<Integer>>() {});
    editLogOperation.getString("commandPrefix");
    editLogOperation.getByteBufferList("data");
    editLogOperation.getString("comment");
    editLogOperation.getString("framework");
    editLogOperation.getString("frameworkVersion");
    editLogOperation.get("dependencyType", DependencyType.class);
    editLogOperation.getInt("dependencyId");
    editLogOperation.getLong("creationTimeMs");
  }

}
