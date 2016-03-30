<%--
~ The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
~ (the “License”). You may not use this work except in compliance with the License, which is
~ available at www.apache.org/licenses/LICENSE-2.0
~
~ This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
~ either express or implied, as more fully set forth in the License.
~
~ See the NOTICE file distributed with this work for information regarding copyright ownership.
--%>

<%@ page import="java.util.*" %>
<%@ page import="alluxio.web.*" %>

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
