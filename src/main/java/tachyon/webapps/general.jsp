<%@ page import="java.util.*" %>
<%@ page import="tachyon.*" %>

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
      <ul class="nav nav-pills">
        <!-- <li class="active"><a href="./home">Master: ${masterNodeAddress}</a></li> -->
        <li class="active"><a href="./home">Master: <%= request.getAttribute("masterNodeAddress") %></a></li>
        <li><a href="./browse?path=/">Browse File System</a></li>
        <li><a href="./memory">View Files in Memory</a></li>
      </ul>
    </div>
  </div>
  <div class ="row-fluid">
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
            <h4>Cluster Summary</h4>
          </a>
        </div>
        <div id="data2" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class="table">
              <tbody>
                <tr>
                  <th>Memory Storage Capacity:</th>
                  <!-- <th>${capacity}</th> -->
                  <th><%= request.getAttribute("capacity") %></th>
                </tr>
                <tr>
                  <th>Memory Storage In-Use</th>
                  <!-- <th>${usedCapacity}</th> -->
                  <th><%= request.getAttribute("usedCapacity") %></th>
                </tr>
                <tr>
                  <th>Workers Running</th>
                  <!-- <th>${liveWorkerNodes}</th> -->
                  <th><%= request.getAttribute("liveWorkerNodes") %></th>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>  
  </div>
  <div class ="row-fluid">
    <div class="accordion span6" id="accordion3">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion3" href="#data3">
            <h4>Pin List</h4>
          </a>
        </div>
        <div id="data3" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class="table">
              <tbody>
                <tr>
                  <!--
                  <c:forEach var="file" items="${pinlist}">
                    <th>${file}</th>
                  </c:forEach>
                  -->
                  <% for (String file : ((List<String>) request.getAttribute("pinlist"))) { %>
                    <tr>  
                      <th><%= file %></th>
                    </tr>
                  <% } %>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>  
    <div class="accordion span6" id="accordion4">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion4" href="#data4">
            <h4>White List</h4>
          </a>
        </div>
        <div id="data4" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class="table">
              <tbody>
                <tr>
                  <!--
                  <c:forEach var="file" items="${whitelist}">
                    <th>${file}</th>
                  </c:forEach>
                  -->
                  <% for (String file : ((List<String>) request.getAttribute("whitelist"))) { %>
                    <tr>
                      <th><%= file %></th>
                    </tr>
                  <% } %>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>  
  </div>
  <div class="row-fluid">
    <div class="accordion span14" id="accordion5">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion5" href="#data5">
            <h4>Detailed Nodes Summary</h4>
          </a>
        </div>
        <div id="data5" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class="table table-hover table-condensed">
              <thead>
                <th>Node Name</th>
                <th>Uptime</th>
                <th>Last Heartbeat</th>
                <th>State</th>
                <th>Capacity</th>
              <tbody>
                <!--
                <c:forEach var="nodeInfo" items="${nodeInfos}">
                  <tr>
                    <th>${nodeInfo.name}</th>
                    <th>${nodeInfo.uptimeClockTime}</th>
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
                    <th><%= nodeInfo.getUptimeClockTime() %></th>
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
      Tachyon is a project developed at the UC Berkeley <a href="https://amplab.cs.berkeley.edu">AMPLab</a>.
    </p>
  </footer>
</div>
</body>
</html>