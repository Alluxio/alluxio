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
  <jsp:include page="header-links.jsp" />
</head>
<title>Alluxio</title>
<body>
<jsp:include page="header-scripts.jsp" />
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
