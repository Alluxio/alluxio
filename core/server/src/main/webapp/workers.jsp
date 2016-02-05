<%@ page import="alluxio.web.*" %>

<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link href="css/bootstrap.min.css" rel="stylesheet" media="screen">
  <link href="css/custom.min.css" rel="stylesheet">
</head>
<title>Workers</title>
<body>
<jsp:include page="header-scripts.jsp" />
<div class="container-fluid">
  <jsp:include page="/header" />
  <div class="row-fluid">
    <div class="accordion span14" id="accordion1">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion1" href="#data1">
            <h4>Live Workers</h4>
          </a>
        </div>
        <div id="data1" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class="table table-hover table-condensed">
              <thead>
                <th>Node Name</th>
                <% if ((Boolean) request.getAttribute("debug")) { %>
                  <th>[D]Uptime</th>
                <% } %>
                <th>Last Heartbeat</th>
                <th>State</th>
                <th>Workers Capacity</th>
                <th>Space Used</th>
                <th>Space Usage</th>
              </thead>
              <tbody>
                <% for (WebInterfaceWorkersServlet.NodeInfo nodeInfo : ((WebInterfaceWorkersServlet.NodeInfo[]) request.getAttribute("normalNodeInfos"))) { %>
                  <tr>
                    <th><a href="http://<%= nodeInfo.getHost() %>:<%= nodeInfo.getWebPort() %>"><%= nodeInfo.getHost() %></a></th>
                    <% if ((Boolean) request.getAttribute("debug")) { %>
                      <th><%= nodeInfo.getUptimeClockTime() %></th>
                    <% } %>
                    <th><%= nodeInfo.getLastHeartbeat() %></th>
                    <th><%= nodeInfo.getState() %></th>
                    <th><%= nodeInfo.getCapacity() %></th>
                    <th><%= nodeInfo.getUsedMemory() %></th>
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
  <div class="row-fluid">
    <div class="accordion span14" id="accordion2">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion2" href="#data2">
            <h4>Lost Workers</h4>
          </a>
        </div>
        <div id="data2" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class="table table-hover table-condensed">
              <thead>
                <th>Node Name</th>
                <% if ((Boolean) request.getAttribute("debug")) { %>
                  <th>[D]Uptime</th>
                <% } %>
                <th>Last Heartbeat</th>
                <th>Workers Capacity</th>
              </thead>
              <tbody>
                <% if (request.getAttribute("failedNodeInfos") != null) {
                  for (WebInterfaceWorkersServlet.NodeInfo nodeInfo : ((WebInterfaceWorkersServlet.NodeInfo[]) request.getAttribute("failedNodeInfos"))) { %>
                  <tr>
                    <th><a href="http://<%= nodeInfo.getHost() %>:<%= request.getAttribute("workerWebPort") %>"><%= nodeInfo.getHost() %></a></th>
                    <% if ((Boolean) request.getAttribute("debug")) { %>
                      <th><%= nodeInfo.getUptimeClockTime() %></th>
                    <% } %>
                    <th><%= nodeInfo.getLastHeartbeat() %></th>
                    <th><%= nodeInfo.getCapacity()%></th>
                  </tr>
                <% }} %>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  </div>
  <%@ include file="footer.jsp" %>
</div>
</body>
</html>
