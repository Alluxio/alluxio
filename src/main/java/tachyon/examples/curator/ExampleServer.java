package tachyon.examples.curator;

import com.google.common.io.Closeables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import java.io.Closeable;
import java.io.IOException;

/**
 * This shows a very simplified method of registering an instance with the service discovery. Each individual
 * instance in your distributed set of applications would create an instance of something similar to ExampleServer,
 * start it when the application comes up and close it when the application shuts down.
 */
public class ExampleServer implements Closeable
{
  private final ServiceDiscovery<InstanceDetails> serviceDiscovery;
  private final ServiceInstance<InstanceDetails> thisInstance;

  public ExampleServer(CuratorFramework client, String path, String serviceName, String description) throws Exception
  {
    // in a real application, you'd have a convention of some kind for the URI layout
    UriSpec     uriSpec = new UriSpec("{scheme}://foo.com:{port}");

    thisInstance = ServiceInstance.<InstanceDetails>builder()
        .name(serviceName)
        .payload(new InstanceDetails(description))
        .port((int)(65535 * Math.random())) // in a real application, you'd use a common port
        .uriSpec(uriSpec)
        .build();

    // if you mark your payload class with @JsonRootName the provided JsonInstanceSerializer will work
    JsonInstanceSerializer<InstanceDetails> serializer = new JsonInstanceSerializer<InstanceDetails>(InstanceDetails.class);

    serviceDiscovery = ServiceDiscoveryBuilder.builder(InstanceDetails.class)
        .client(client)
        .basePath(path)
        .serializer(serializer)
        .thisInstance(thisInstance)
        .build();
  }

  public ServiceInstance<InstanceDetails> getThisInstance()
  {
    return thisInstance;
  }

  public void start() throws Exception
  {
    serviceDiscovery.start();
  }

  @Override
  public void close() throws IOException
  {
    Closeables.closeQuietly(serviceDiscovery);
  }
}