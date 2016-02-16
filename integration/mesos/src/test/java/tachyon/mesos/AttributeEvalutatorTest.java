package tachyon.mesos;

import org.apache.mesos.Protos;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AttributeEvalutatorTest {

  private List<Protos.Attribute> attrs = new ArrayList<Protos.Attribute>();
  private AttributeEvaluator evaluator;

  @Before
  public void setUp() throws Exception {
    attrs.add(Protos.Attribute.newBuilder()
            .setType(Protos.Value.Type.TEXT)
            .setName("colo")
            .setText(Protos.Value.Text.newBuilder().setValue("sparkonly").build()).build());
    attrs.add(Protos.Attribute.newBuilder()
            .setType(Protos.Value.Type.TEXT)
            .setName("type")
            .setText(Protos.Value.Text.newBuilder().setValue("server").build()).build());
    attrs.add(Protos.Attribute.newBuilder()
            .setType(Protos.Value.Type.TEXT)
            .setName("network")
            .setText(Protos.Value.Text.newBuilder().setValue("none").build()).build());
    attrs.add(Protos.Attribute.newBuilder()
            .setType(Protos.Value.Type.TEXT)
            .setName("core")
            .setText(Protos.Value.Text.newBuilder().setValue("32").build()).build());

    evaluator = new AttributeEvaluator(this.attrs);
  }

  @Test
  public void matchSingleAttributeTest() throws Exception {
    assertTrue("Should match attribute here", this.evaluator.matchAttributes("colo:sparkonly"));
  }

  @Test
  public void matchAndAttributesTest() throws Exception {
    assertTrue("Should match attribute here", this.evaluator.matchAttributes("colo:sparkonly & type:server"));
  }

  @Test
  public void matchOrAttributesTest() throws Exception {
    assertTrue("Should match attribute here", this.evaluator.matchAttributes("colo:sparkonly | type:vm"));
  }

  @Test
  public void PriorityTest() throws Exception {
    assertFalse("Should not match attribute here", this.evaluator.matchAttributes("network:calico & colo:sparkonly | type:vm"));
    assertTrue("Should match attribute here", this.evaluator.matchAttributes("network:calico & colo:sparkonly | type:server"));
  }
}
