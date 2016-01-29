/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.lineage;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import tachyon.Constants;
import tachyon.master.TachyonMaster;

/**
 * This class is a REST handler for lineage master requests.
 */
@Path("/")
public class LineageMasterRestServiceHandler {

  /**
   * @return the response object
   */
  @GET
  @Path("lineage/service_name")
  @Produces(MediaType.APPLICATION_JSON)
  public Response name() {
    return Response.ok(Constants.LINEAGE_MASTER_CLIENT_SERVICE_NAME).build();
  }

  /**
   * @return the response object
   */
  @GET
  @Path("lineage/service_version")
  @Produces(MediaType.APPLICATION_JSON)
  public Response version() {
    return Response.ok(Constants.LINEAGE_MASTER_CLIENT_SERVICE_VERSION).build();
  }

  /**
   * @return the response object
   */
  @GET
  @Path("lineage/test")
  @Produces(MediaType.APPLICATION_JSON)
  public Response test() {
    LineageMaster master = TachyonMaster.get().getLineageMaster();
    return Response.ok().build();
  }
}
