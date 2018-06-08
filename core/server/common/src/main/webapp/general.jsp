<%--

    The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
    (the "License"). You may not use this work except in compliance with the License, which is
    available at www.apache.org/licenses/LICENSE-2.0

    This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
    either express or implied, as more fully set forth in the License.

    See the NOTICE file distributed with this work for information regarding copyright ownership.

--%>
<%@ page import="java.util.*" %>
<%@ page import="java.lang.*" %>
<%@ page import="alluxio.web.*" %>
<%@ page import="alluxio.wire.*" %>

<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <jsp:include page="header-links.jsp" />
</head>
<title>Alluxio</title>
<body>
<jsp:include page="header-scripts.jsp" />
<div class="container-fluid">
  <jsp:include page="/header" />
  <div class="row-fluid">
    <div class="accordion span6" id="accordion1">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion1" href="#data1">
            <h4>Alluxio Summary</h4>
          </a>
        </div>
        <div id="data1" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class="table">
              <tbody>
                <tr>
                  <th>Master Address:</th>
                  <th><%= request.getAttribute("masterNodeAddress") %></th>
                </tr>
                <tr>
                  <th>Started:</th>
                  <!-- <th>${startTime}</th> -->
                  <th><%= request.getAttribute("startTime") %></th>
                </tr>
                <tr>
                  <th>Uptime:</th>
                  <!-- <th>${uptime}</th> -->
                  <th><%= request.getAttribute("uptime") %></th>
                </tr>
                <tr>
                  <th>Version:</th>
                  <!-- <th>${version}</th> -->
                  <th><%= request.getAttribute("version") %></th>
                </tr>
                <tr>
                  <th>Running Workers:</th>
                  <!-- <th>${liveWorkerNodes}</th> -->
                  <th><%= request.getAttribute("liveWorkerNodes") %></th>
                </tr>
                <tr>
                  <th>Startup Consistency Check:</th>
                  <th><%= request.getAttribute("consistencyCheckStatus") %></th>
                </tr>
                <% if ((Integer) request.getAttribute("inconsistentPaths") != 0) { %>
                  <tr>
                    <th><font color="red">Inconsistent Files on Startup (run fs checkConsistency for details):</font></th>
                    <th><font color="red"><%= request.getAttribute("inconsistentPaths") %></font></th>
                  </tr>
                <% } %>
                <tr>
                  <% ConfigCheckReport configReport = ((ConfigCheckReport) request.getAttribute("configCheckReport")); %>
                  <% ConfigCheckReport.ConfigStatus status = (ConfigCheckReport.ConfigStatus) request.getAttribute("configCheckStatus");%>
                  <% if (status.equals(ConfigCheckReport.ConfigStatus.FAILED)) { %>
                    <th><font color="red">Server Configuration Check:<font color="red"></th>
                    <th><font color="red"><%= status %><font color="red"></th>
                  <% } else { %>
                    <th>Server Configuration Check:</th>
                    <th><%= status %></th>
                  <% } %>
                </tr>
                <% int errorSize = (int) request.getAttribute("configCheckErrorNum"); %>
                <% int warnSize = (int) request.getAttribute("configCheckWarnNum"); %>
                <% int inconsistentProperties = errorSize + warnSize; %>
                <% if (inconsistentProperties != 0) { %>
                  <tr>
                    <% if (errorSize != 0) { %>
                      <th><font color="red">Inconsistent Properties:</font></th>
                      <th><font color="red"><%= inconsistentProperties %></font></th>
                    <% } else { %>
                      <th>Inconsistent Properties:</th>
                      <th><%= inconsistentProperties %></th>
                    <% } %>
                  </tr>
                <% } %>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>

    <div class="accordion span6" id="accordion2">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion2" href="#data2">
            <h4>Cluster Usage Summary</h4>
          </a>
        </div>
        <div id="data2" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class="table">
              <tbody>
                <tr>
                  <th>Workers Capacity:</th>
                  <!-- <th>${capacity}</th> -->
                  <th><%= request.getAttribute("capacity") %></th>
                </tr>
                <tr>
                  <th>Workers Free / Used:</th>
                  <!-- <th>${usedCapacity}</th> -->
                  <th><%= request.getAttribute("freeCapacity") %> / <%= request.getAttribute("usedCapacity") %></th>
                </tr>
                <tr>
                  <th>UnderFS Capacity:</th>
                  <!-- <th>${capacity}</th> -->
                  <th><%= request.getAttribute("diskCapacity") %></th>
                </tr>
                <tr>
                  <th>UnderFS Free / Used:</th>
                  <!-- <th>${freeCapacity}</th> -->
                  <th><%= request.getAttribute("diskFreeCapacity") %> / <%= request.getAttribute("diskUsedCapacity") %></th>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  </div>
  <!--  show storage usage summary -->
  <div class="row-fluid">
    <div class="accordion span14" id="accordion3">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion3" href="#data3">
            <h4>Storage Usage Summary</h4>
          </a>
        </div>
        <div id="data3" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class="table table-hover table-condensed">
              <thead>
                <th>Storage Alias</th>
                <th>Space Capacity</th>
                <th>Space Used</th>
                <th>Space Usage</th>
              </thead>
              <tbody>
                <% for (WebInterfaceGeneralServlet.StorageTierInfo info : ((WebInterfaceGeneralServlet.StorageTierInfo[]) request.getAttribute("storageTierInfos"))) { %>
                  <tr>
                    <th><%= info.getStorageTierAlias() %></th>
                    <th><%= info.getCapacity() %></th>
                    <th><%= info.getUsedCapacity() %></th>
                    <th>
                      <div class="progress custom-progress">
                        <div class="bar bar-success" style="width: <%= info.getFreeSpacePercent() %>%;">
                          <% if (info.getFreeSpacePercent() >= info.getUsedSpacePercent()) { %>
                            <%= info.getFreeSpacePercent() %>%Free
                          <% } %>
                        </div>
                        <div class="bar bar-danger" style="width: <%= info.getUsedSpacePercent() %>%;">
                          <% if (info.getFreeSpacePercent() < info.getUsedSpacePercent()) { %>
                            <%= info.getUsedSpacePercent() %>%Used
                          <% } %>
                        </div>
                      </div>
                    </th>
                  </tr>
                <% } %>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  </div>
  <!--  show inconsistent paths  -->
  <% if ((Integer) request.getAttribute("inconsistentPaths") != 0) { %>
  <div class="row-fluid">
    <div class="accordion span14" id="accordion4">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion4" href="#data4">
            <h4>Inconsistent Files Details</h4>
          </a>
        </div>
        <div id="data4" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class="table table-hover table-condensed">
              <thead>
                <th> <font color="red">On Startup, <%= request.getAttribute("inconsistentPaths") %> inconsistent files were found. This check is only checked once at startup, and you can restart the Alluxio Master for the latest information. <br/> The following files may be corrupted: </font></th>
              </thead>
              <tbody>
                <% List array = (List) request.getAttribute("inconsistentPathItems");
                   for (int i = 0; i < array.size(); i++) { %>
                  <tr>
                    <th><font color="red"><%= array.get(i) %></font></th>
                  </tr>
                <% }%>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  </div>
  <% } %>
  <!--  show inconsistent configuration  -->
  <% if (inconsistentProperties != 0) { %>
  <div class="row-fluid">
    <div class="accordion span12" id="accordion5">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion5" href="#data5">
            <h4>Inconsistent Properties Details</h4>
          </a>
        </div>
        <div id="data5" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class="table table-hover table-condensed">
              <thead>
                <th class="span2">Scope</th>
                <th class="span4">Property</th>
                <th class="span6">Value</th>
              </thead>
              <tbody>
                <% if (errorSize != 0) { %>
                  <tr>
                    <th colspan="3"> <font color="red">Errors (those properties are required to be identical)</font></th>
                  </tr>
                  <% for (Map.Entry<Scope, List<InconsistentProperty>> error : ((Map<Scope, List<InconsistentProperty>>) request.getAttribute("configCheckErrors")).entrySet()) { %>
                    <% for (InconsistentProperty inconsistentProperty: error.getValue()) { %>
                      <% String scope = error.getKey().toString(); %>
                      <% String name = inconsistentProperty.getName(); %>
                      <% for (Map.Entry<Optional<String>, List<String>> entry : inconsistentProperty.getValues().entrySet()) { %>
                        <tr>
                          <th class="span2"><font color="red"><%= scope %></font></th>
                          <th class="span4"><font color="red"><%= name %></font></th>
                          <% String value = String.format("%s (%s)", entry.getKey().orElse("no value set"), String.join(", ", entry.getValue()));%>
                          <th class="span8"><font color="red"><%= value %></font></th>
                        </tr>
                        <% scope = ""; %>
                        <% name = ""; %>
                      <% } %>
                    <% } %>
                  <% } %>
                <% } %>
                <% if (warnSize != 0) { %>
                  <tr>
                    <th colspan="3">Warnings (those properties are recommended to be identical)</th>
                  </tr>
                  <% for (Map.Entry<Scope, List<InconsistentProperty>> warn : ((Map<Scope, List<InconsistentProperty>>) request.getAttribute("configCheckWarns")).entrySet()) { %>
                    <% for (InconsistentProperty inconsistentProperty: warn.getValue()) { %>
                      <% String scope = warn.getKey().toString(); %>
                      <% String name = inconsistentProperty.getName(); %>
                      <% for (Map.Entry<Optional<String>, List<String>> entry : inconsistentProperty.getValues().entrySet()) { %>
                        <tr>
                          <th class="span2"><%= scope %></th>
                          <th class="span4"><%= name %></th>
                          <% String value = String.format("%s (%s)", entry.getKey().orElse("no value set"), String.join(", ", entry.getValue()));%>
                          <th class="span8"><%= value %></th>
                        </tr>
                        <% scope = ""; %>
                        <% name = ""; %>
                      <% } %>
                    <% } %>
                  <% } %>
                <% } %>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  </div>
  <% } %>
<!--  Hide variables for now
  <div class="row-fluid">
    <div class="accordion span14" id="accordion5">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion5" href="#data5">
            <h4>Recomputation Variables</h4>
          </a>
        </div>
        <div id="data5" class="accordion-body collapse in">
          <div class="accordion-inner">
            <form class="form" method="post" action="/home">
              <div id="recomputationVars">
              < % int i = 0; %>
              < % for (String key : ((Hashtable<String, String>) request.getAttribute("recomputeVariables")).keySet()) { %>
                <div id="varDiv< %= i %>">
                  <div class="input-prepend">
                    <span class="add-on">Name</span>
                    <input class="span8" name="varName< %= i %>" type="text" value="< %= key %>">
                  </div>
                  <div class="input-prepend">
                    <span class="add-on">Value</span>
                    <input class="span8" type="text" name="varVal< %= i %>" value="< %= ((Hashtable<String, String>) request.getAttribute("recomputeVariables")).get(key) %>">
                    <input style="margin-left:10px" class="btn btn-danger" type="button" value="Delete" onclick="deleteVar(varDiv< %= i++ %>)"/>
                  </div>
                </br>
                </div>
              < % } %>
              </div>
              <div class="form-actions">
                <input class="btn btn-primary" type="submit" value="Submit" id="submit"/>
                <input class="btn" type="button" value="Add Variable" id="addVariable"/>
              </div>
            </form>
          </div>
        </div>
      </div>
    </div>
  </div>
-->
  <%@ include file="footer.jsp" %>
</div>
<script>
var id = ($("#recomputationVars").children().length).toString();
$(document).ready(function () {
  $("#addVariable").click(function () {
    $("#recomputationVars").append('<div id="varDiv' + id + '"><div class="input-prepend" style="padding-right: 4px"><span class="add-on">Name</span><input class="span8" name="varName' + id + '" type="text" value="Variable Name"></div><div class="input-prepend"><span class="add-on">Value</span><input class="span8" type="text" name="varVal' + id + '" value="Value"><input style="margin-left:10px" class="btn btn-danger" type="button" value="Delete" onclick="deleteVar(varDiv' + id + ')"/></div></br></div>');
    id ++;
  });
});

function deleteVar(value) {
	value.remove();
}
</script>
</body>
</html>
