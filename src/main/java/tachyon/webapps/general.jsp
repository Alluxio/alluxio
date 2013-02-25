<%@ page isELIgnored ="false" %> 
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>  
<html>
  <head>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link href="bootstrap/css/bootstrap.min.css" rel="stylesheet" media="screen">
  </head>
  <title>Tachyon</title>
  <body>
    <script src="http://code.jquery.com/jquery-latest.min.js" type="text/javascript"></script>
    <script src="bootstrap/js/bootstrap.min.js"></script>
    <div class="container-fluid">
      <div class="navbar navbar-inverse">
        <div class="navbar-inner">
          <ul class="nav nav-pills">
            <li class="active"><a href="./home">Master Node</a></li>
            <li><a href="./browse?path=/">Browse File System</a></li>
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
                  <th>${startTime}</th>
                </tr>
                <tr>
                  <th>Uptime:</th>
                  <th>${uptime}</th>
                </tr>
                <tr>
                  <th>Version:</th>
                  <th>${version}</th>
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
                    <th>${capacity}</th>
                  </tr>
                  <tr>
                    <th>Memory Storage In-Use</th>
                    <th>${usedCapacity}</th>
                  </tr>
                  <tr>
                    <th>Workers Running</th>
                    <th>${liveWorkerNodes}</th>
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
            <a class="accordion-toggle" data-toggle="collapse" 
              data-parent="#accordion3" href="#data3">
              <h4>Pin List</h4>
            </a>
          </div>
          <div id="data3" class="accordion-body collapse in">
            <div class="accordion-inner">
              <table class="table">
                <tbody>
                  <tr>
                    <c:forEach var="file" items="${pinlist}">
                      <th>${file}</th>
                    </c:forEach>
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
            <a class="accordion-toggle" data-toggle="collapse" 
              data-parent="#accordion4" href="#data4">
              <h4>White List</h4>
            </a>
          </div>
          <div id="data4" class="accordion-body collapse in">
            <div class="accordion-inner">
              <table class="table">
                <tbody>
                  <tr>
                    <c:forEach var="file" items="${whitelist}">
                      <th>${file}</th>
                    </c:forEach>
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
            <a class="accordion-toggle" data-toggle="collapse" 
              data-parent="#accordion5" href="#data5">
              <h4>Detailed Nodes Summary</h4>
            </a>
          </div>
          <div id="data5" class="accordion-body collapse in">
            <div class="accordion-inner">
              <table class="table table-hover">
                <thead>
                  <th>Node Name</th>
                  <th>Last Heartbeat</th>
                  <th>State</th>
                  <th>Capacity</th>
                <tbody>
                  <c:forEach var="nodeInfo" items="${nodeInfos}">
                    <tr>
                      <th>${nodeInfo.name}</th>
                      <th>${nodeInfo.lastHeartbeat} seconds ago</th>
                      <th>${nodeInfo.state}</th>
                      <th>
                        <div class="progress">
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
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
      </div>  
    </div>
  </body>
</html>