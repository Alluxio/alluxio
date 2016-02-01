<%@ page import="java.util.*" %>
<%@ page import="tachyon.web.*" %>
<%@ page import="tachyon.web.*" %>

<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link href="../css/bootstrap.min.css" rel="stylesheet" media="screen">
  <link href="../css/tachyoncustom.min.css" rel="stylesheet">
  <link href="../css/linearGraph.css" rel="stylesheet">
</head>
<title>Tachyon</title>
<body>
<script src="../js/jquery-1.9.1.min.js" type="text/javascript"></script>
<script src="../js/bootstrap.min.js"></script>
<script src="../js/d3.min.js" type="text/javascript"></script>
<script src="../js/linearGraph.js" type="text/javascript"></script>
<div class="container-fluid">
  <% request.setAttribute("useWorkerHeader", "1"); %>
  <jsp:include page="/header" />

    <div class="row-fluid">
      <div class="accordion span14" id="accordion3">
        <div class="accordion-group">
          <div class="accordion-heading">
            <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion3" href="#data3">
              <h4>Worker gauges</h4>
            </a>
          </div>
          
          <div id="data3" class="accordion-body collapse in">
            <div class="accordion-inner">
              <table class="table table-hover table-condensed">
                <thead>                  
                  <th></th>                  
                  <th></th>
                </thead>
                <tbody>
                  <tr>                    
                    <th>Worker Capacity</th>                    
                    <th>
                      <div class="progress custom-progress">
                        <div class="bar bar-success" style="width: <%= (Integer) request.getAttribute("workerCapacityFreePercentage") %>%;">
                          <% if ( (Integer) request.getAttribute("workerCapacityFreePercentage") >= (Integer) request.getAttribute("workerCapacityUsedPercentage")) { %>
                            <%= request.getAttribute("workerCapacityFreePercentage") %>%Free
                          <% } %>
                        </div>
                        <div class="bar bar-danger" style="width: <%= (Integer) request.getAttribute("workerCapacityUsedPercentage") %>%;">
                          <% if ((Integer) request.getAttribute("workerCapacityFreePercentage") < (Integer) request.getAttribute("workerCapacityUsedPercentage")) { %>
                            <%= request.getAttribute("workerCapacityUsedPercentage") %>%Used
                          <% } %>
                        </div>
                      </div>
                    </th>
                  </tr>                                    
                </tbody>
              </table>
            </div>
          </div>
          
        </div>
      </div>
    </div>        
    
    <div class="row-fluid">
      <jsp:include page="metrics-history.jsp">
        <jsp:param value="${operationMetrics}" name="operationMetrics" />
      </jsp:include>    
    </div>
  <%@ include file="../footer.jsp" %>
</div>
</body>
</html>