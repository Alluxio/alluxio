<%@ page import="java.util.*" %>
<%@ page import="tachyon.web.*" %>

<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link href="css/bootstrap.min.css" rel="stylesheet" media="screen">
  <link href="css/tachyoncustom.min.css" rel="stylesheet">
</head>
<title>Tachyon</title>
<body>
<script src="js/jquery-1.9.1.min.js" type="text/javascript"></script>
<script src="js/bootstrap.min.js"></script>
<div class="container-fluid">
  <div class="navbar navbar-inverse">
    <div class="navbar-inner">
      <ul class="nav nav-pills" style="height:40px;font-size:14px;font-weight: bold;vertical-align: bottom;">
        <li><a href="http://tachyon-project.org/" target="_blank"><img style="height:25px;margin-top:-5px;" src="img/logo.png" alt="Tachyon Logo"/></a></li>
        <li class="active"><a href="./home">Overview</a></li>
        <li><a href="./configuration">System Configuration</a></li>
        <li><a href="./browse?path=/">Browse File System</a></li>
        <li><a href="./memory">In Memory Files</a></li>
      </ul>
    </div>
  </div>
  <div class="row-fluid">
    <div class="accordion span6" id="accordion1">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion1" href="#data1">
            <h4>Tachyon Summary</h4>
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
                  <th>Memory Capacity:</th>
                  <!-- <th>${capacity}</th> -->
                  <th><%= request.getAttribute("capacity") %></th>
                </tr>
                <tr>
                  <th>Memory Free / Used:</th>
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
              <% int i = 0; %>
              <% for (String key : ((Hashtable<String, String>) request.getAttribute("recomputeVariables")).keySet()) { %>
                <div id="varDiv<%= i %>">
                  <div class="input-prepend">
                    <span class="add-on">Name</span>
                    <input class="span8" name="varName<%= i %>" type="text" value="<%= key %>">
                  </div>
                  <div class="input-prepend">
                    <span class="add-on">Value</span>
                    <input class="span8" type="text" name="varVal<%= i %>" value="<%= ((Hashtable<String, String>) request.getAttribute("recomputeVariables")).get(key) %>">
                    <input style="margin-left:10px" class="btn btn-danger" type="button" value="Delete" onclick="deleteVar(varDiv<%= i++ %>)"/>
                  </div>
                </br>
                </div>
              <% } %>
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
  <div class="row-fluid">
    <div class="accordion span14" id="accordion6">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion6" href="#data6">
            <h4>Detailed Nodes Summary</h4>
          </a>
        </div>
        <div id="data6" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class="table table-hover table-condensed">
              <thead>
                <th>Node Name</th>
                <!--
                  <c:if test="${debug}">
                    <th>[D]Uptime</th>
                  </c:if>
                  -->
                <% if ((Boolean) request.getAttribute("debug")) { %>
                  <th>[D]Uptime</th>
                <% } %>
                <th>Last Heartbeat</th>
                <th>State</th>
                <th>Memory Usage</th>
              <tbody>
                <!--
                <c:forEach var="nodeInfo" items="${nodeInfos}">
                  <tr>
                    <th>${nodeInfo.name}</th>
                    <c:if test="${debug}">
                      <th>${nodeInfo.uptimeClockTime}</th>
                    </c:if>
                    <th>${nodeInfo.lastHeartbeat} seconds ago</th>
                    <th>${nodeInfo.state}</th>
                    <th>
                      <div class="progress custom-progress">
                          <div class="bar bar-success" style="width: ${nodeInfo.freeSpacePercent}%;">
                            <c:if test="${nodeInfo.freeSpacePercent ge nodeInfo.usedSpacePercent}">
                              ${nodeInfo.freeSpacePercent}% Free
                            </c:if>
                          </div>
                          <div class="bar bar-danger" style="width: ${nodeInfo.usedSpacePercent}%;">
                            <c:if test="${nodeInfo.usedSpacePercent gt nodeInfo.freeSpacePercent}">
                              ${nodeInfo.usedSpacePercent}% Used
                            </c:if>
                          </div>
                      </div>
                    </th>
                  </tr>
                </c:forEach>
                -->
                <% for (WebInterfaceGeneralServlet.NodeInfo nodeInfo : ((WebInterfaceGeneralServlet.NodeInfo[]) request.getAttribute("nodeInfos"))) { %>
                  <tr>
                    <th><%= nodeInfo.getName() %></th>
                    <% if ((Boolean) request.getAttribute("debug")) { %>
                      <th><%= nodeInfo.getUptimeClockTime() %></th>
                    <% } %>
                    <th><%= nodeInfo.getLastHeartbeat() %></th>
                    <th><%= nodeInfo.getState() %></th>
                    <th>
                      <div class="progress custom-progress">
                          <div class="bar bar-success" style="width: <%= nodeInfo.getFreeSpacePercent() %>%;">
                            <% if (nodeInfo.getFreeSpacePercent() >= nodeInfo.getUsedSpacePercent()) { %>
                              <%= nodeInfo.getFreeSpacePercent() %>%Free
                            <% } %>
                          </div>
                          <div class="bar bar-danger" style="width: <%= nodeInfo.getUsedSpacePercent() %>%;">
                            <% if (nodeInfo.getFreeSpacePercent() < nodeInfo.getUsedSpacePercent()) { %>
                              <%= nodeInfo.getUsedSpacePercent() %>%Used
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
  <footer>
    <p style="text-align: center;">
      <a href="http://tachyon-project.org/" target="_blank">Tachyon</a> is an <a href="https://github.com/amplab/tachyon" target="_blank">open source</a> project developed at the UC Berkeley <a href="https://amplab.cs.berkeley.edu" target="_blank">AMPLab</a>.
    </p>
  </footer>
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
