package alluxio.master;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.qmino.miredot.annotations.ReturnType;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.Version;

/**
 * This class is a REST handler for requesting general master information.
 */
@NotThreadSafe
@Path(AlluxioMasterRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
// TODO(cc): Investigate auto-generation of REST API documentation.
public final class AlluxioMasterRestServiceHandler {
  public static final String SERVICE_PREFIX = "master";
  public static final String GET_CONFIGURATION = "configuration";
  public static final String GET_DEBUG = "debug";
  public static final String GET_ADDRESS = "address";
  public static final String GET_START_TIME_MS = "start_time_ms";
  public static final String GET_UPTIME_MS = "uptime_ms";
  public static final String GET_VERSION = "version";

  private final AlluxioMaster mMaster = AlluxioMaster.get();
  private final Configuration mMasterConf = MasterContext.getConf();

  /**
   * @summary get the configuration map
   * @return the response object
   */
  @GET
  @Path(GET_CONFIGURATION)
  @ReturnType("java.util.Map<String, String>")
  public Response getConfiguration() {
    Set<Map.Entry<Object, Object>> properties = mMasterConf.getInternalProperties().entrySet();
    Map<String, String> configuration = new HashMap<>(properties.size());
    for (Map.Entry<Object, Object> entry : properties) {
      String key = entry.getKey().toString();
      configuration.put(key, mMasterConf.get(key));
    }
    return Response.ok(configuration).build();
  }

  /**
   * @summary get whether the master is in debug mode
   * @return the response object
   */
  @GET
  @Path(GET_DEBUG)
  @ReturnType("java.lang.Boolean")
  public Response isDebug() {
    return Response.ok(mMasterConf.getBoolean(Constants.DEBUG)).build();
  }

  /**
   * @summary get the master address
   * @return the response object
   */
  @GET
  @Path(GET_ADDRESS)
  @ReturnType("java.lang.String")
  public Response getAddress() {
    return Response.ok(mMasterConf.get(Constants.MASTER_ADDRESS)).build();
  }

  /**
   * @summary get the uptime of the master
   * @return the response object
   */
  @GET
  @Path(GET_UPTIME_MS)
  @ReturnType("java.lang.Long")
  public Response getUptimeMs() {
    return Response.ok(System.currentTimeMillis() - mMaster.getStarttimeMs()).build();
  }

  /**
   * @summary get the start time of the master
   * @return the response object
   */
  @GET
  @Path(GET_START_TIME_MS)
  @ReturnType("java.lang.Long")
  public Response getStartTimeMs() {
    return Response.ok(mMaster.getStarttimeMs()).build();
  }

  /**
   * @summary get the version of the master
   * @return the response object
   */
  @GET
  @Path(GET_VERSION)
  @ReturnType("java.lang.String")
  public Response getVersion() {
    return Response.ok(Version.VERSION).build();
  }
}
