<%@ page import="java.util.*" %>
<%@ page import="tachyon.StorageDirId" %>
<%@ page import="tachyon.StorageLevelAlias" %>
<%@ page import="tachyon.util.*" %>
<%@ page import="tachyon.web.WebInterfaceWorkerGeneralServlet.UiStorageDir" %>

<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link href="../css/bootstrap.min.css" rel="stylesheet" media="screen">
  <link href="../css/tachyoncustom.min.css" rel="stylesheet">
</head>
<title>Tachyon</title>
<body>
<script src="../js/jquery-1.9.1.min.js" type="text/javascript"></script>
<script src="../js/bootstrap.min.js"></script>
<div class="container-fluid">
  <% request.setAttribute("useWorkerHeader", "1"); %>
  <jsp:include page="/header" />
  <div class="row-fluid">
    <div class="accordion span6" id="accordion1">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion1" href="#data1">
            <h4>Worker Summary</h4>
          </a>
        </div>
        <div id="data1" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class="table">
              <tbody>
                <tr>
                  <th>Worker Address:</th>
                  <th><%= request.getAttribute("workerAddress") %></th>
                </tr>
                <tr>
                  <th>Started:</th>
                  <th><%= request.getAttribute("startTime") %></th>
                </tr>
                <tr>
                  <th>Uptime:</th>
                  <th><%= request.getAttribute("uptime") %></th>
                </tr>
                <tr>
                  <th>Version:</th>
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
            <h4>Storage Usage Summary</h4>
          </a>
        </div>
        <div id="data2" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class="table">
              <tbody>
                <tr>
                  <th>Total Capacity / Used</th>
                  <th><%= request.getAttribute("capacityBytes") %> / <%= request.getAttribute("usedBytes") %></th>
                </tr>
                <% List<Long> capacityBytesOnTiers = (List<Long>) request.getAttribute("capacityBytesOnTiers"); %>
                <% List<Long> usedBytesOnTiers = (List<Long>) request.getAttribute("usedBytesOnTiers"); %>
                <% for (int i = 0; i < capacityBytesOnTiers.size(); i ++) { %>
                  <% if (capacityBytesOnTiers.get(i) > 0) { %>
                  <tr>
                    <th><%= StorageLevelAlias.values()[i].name() %> Capacity / Used</th>
                    <th>
                      <%= CommonUtils.getSizeFromBytes(capacityBytesOnTiers.get(i)) %> /
                      <%= CommonUtils.getSizeFromBytes(usedBytesOnTiers.get(i)) %>
                    </th>
                  </tr>
                  <% } %>
                <% } %>
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
            <h4>Tiered Storage Details</h4>
          </a>
        </div>
        <div id="data3" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class="table table-hover table-condensed">
              <thead>
                <th>Alias</th>
                <th>Path</th>
                <th>Capacity</th>
                <th>Space Used</th>
                <th>Space Usage</th>
              </thead>
              <tbody>
                <% for (UiStorageDir dir : ((List<UiStorageDir>) request.getAttribute("storageDirs"))) { %>
                  <tr>
                    <th><%= StorageDirId.getStorageLevelAlias(dir.getStorageDirId()) %></th>
                    <th><%= dir.getDirPath() %></th>
                    <th><%= CommonUtils.getSizeFromBytes(dir.getCapacityBytes()) %></th>
                    <th><%= CommonUtils.getSizeFromBytes(dir.getUsedBytes()) %></th>
                    <th>
                      <div class="progress custom-progress">
                        <% int usedSpacePercent = (int) (100.0 * dir.getUsedBytes() / dir.getCapacityBytes()); %>
                        <% int freeSpacePercent = 100 - usedSpacePercent; %>
                        <div class="bar bar-success" style="width: <%= freeSpacePercent %>%;">
                          <% if (freeSpacePercent >= usedSpacePercent) { %>
                            <%= freeSpacePercent %>%Free
                          <% } %>
                        </div>
                        <div class="bar bar-danger" style="width: <%= usedSpacePercent %>%;">
                          <% if (freeSpacePercent < usedSpacePercent) { %>
                            <%= usedSpacePercent %>%Used
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
  <%@ include file="../footer.jsp" %>
</div>
</body>
</html>
