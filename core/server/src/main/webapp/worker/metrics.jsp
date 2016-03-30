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
<%@ page import="alluxio.web.*" %>

<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <jsp:include page="../header-links.jsp" />
</head>
<title>Alluxio</title>
<body>
<script src="../js/jquery-1.9.1.min.js" type="text/javascript"></script>
<script src="../js/bootstrap.min.js"></script>
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
        <jsp:include page="metrics-summary.jsp" />
    </div>
  <%@ include file="../footer.jsp" %>
</div>
</body>
</html>
