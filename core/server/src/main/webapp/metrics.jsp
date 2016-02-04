<%@ page import="java.util.*" %>
<%@ page import="tachyon.web.*" %>
<%@ page import="tachyon.web.*" %>

<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link href="css/bootstrap.min.css" rel="stylesheet" media="screen">
  <link href="css/tachyoncustom.min.css" rel="stylesheet">
  <link href="css/linearGraph.css" rel="stylesheet">
</head>
<title>Tachyon</title>
<body>
<jsp:include page="header-scripts.jsp" />
<script src="js/d3.min.js" type="text/javascript"></script>
<script src="js/linearGraph.js" type="text/javascript"></script>
<div class="container-fluid">
  <jsp:include page="/header" />

    <div class="row-fluid">
      <div class="accordion span14" id="accordion3">
        <div class="accordion-group">
          <div class="accordion-heading">
            <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion3" href="#data3">
              <h4>Master gauges</h4>
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
                    <th>Master Capacity</th>                    
                    <th>
                      <div class="progress custom-progress">
                        <div class="bar bar-success" style="width: <%= (Integer) request.getAttribute("masterCapacityFreePercentage") %>%;">
                          <% if ( (Integer) request.getAttribute("masterCapacityFreePercentage") >= (Integer) request.getAttribute("masterCapacityUsedPercentage")) { %>
                            <%= request.getAttribute("masterCapacityFreePercentage") %>%Free
                          <% } %>
                        </div>
                        <div class="bar bar-danger" style="width: <%= (Integer) request.getAttribute("masterCapacityUsedPercentage") %>%;">
                          <% if ((Integer) request.getAttribute("masterCapacityFreePercentage") < (Integer) request.getAttribute("masterCapacityUsedPercentage")) { %>
                            <%= request.getAttribute("masterCapacityUsedPercentage") %>%Used
                          <% } %>
                        </div>
                      </div>
                    </th>
                  </tr>
                  
                  <tr>                    
                    <th>Master Underfs Capacity</th>                    
                    <th>
                      <div class="progress custom-progress">
                        <div class="bar bar-success" style="width: <%= (Integer) request.getAttribute("masterUnderfsCapacityFreePercentage") %>%;">
                          <% if ( (Integer) request.getAttribute("masterUnderfsCapacityFreePercentage") >= (Integer) request.getAttribute("masterUnderfsCapacityUsedPercentage")) { %>
                            <%= request.getAttribute("masterUnderfsCapacityFreePercentage") %>%Free
                          <% } %>
                        </div>
                        <div class="bar bar-danger" style="width: <%= (Integer) request.getAttribute("masterUnderfsCapacityUsedPercentage") %>%;">
                          <% if ((Integer) request.getAttribute("masterUnderfsCapacityFreePercentage") < (Integer) request.getAttribute("masterUnderfsCapacityUsedPercentage")) { %>
                            <%= request.getAttribute("masterUnderfsCapacityUsedPercentage") %>%Used
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
        <jsp:include page="metrics-summary.jsp" />
    </div>
  <%@ include file="footer.jsp" %>
</div>
</body>
</html>