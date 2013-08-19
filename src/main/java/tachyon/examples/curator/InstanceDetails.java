package tachyon.examples.curator;

import org.codehaus.jackson.map.annotate.JsonRootName;

/**
 * In a real application, the Service payload will most likely
 * be more detailed than this. But, this gives a good example.
 */
@JsonRootName("details")
public class InstanceDetails
{
  private String        description;

  public InstanceDetails()
  {
    this("");
  }

  public InstanceDetails(String description)
  {
    this.description = description;
  }

  public void setDescription(String description)
  {
    this.description = description;
  }

  public String getDescription()
  {
    return description;
  }
}