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
<%@ page import="org.apache.commons.lang3.tuple.*" %>

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

  </div>
  <div class="row-fluid">
    <div class="accordion span14" id="accordion8">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion8" href="#data8">
            <h4>Alluxio Configuration</h4>
          </a>
        </div>
        <div id="data8" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class = "table">
              <tbody>
                <% for (ImmutablePair<String, String> entry : (SortedSet<ImmutablePair<String, String>>) request.getAttribute("configuration")) { %>
                  <tr>
                    <th><%= entry.getLeft() %></th>
                    <th><%= entry.getRight() %></th>
                  </tr>
                <% } %>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  </div>
  <div class ="row-fluid">
    <div class="accordion span14" id="accordion7">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion7" href="#data7">
            <h4>White List</h4>
          </a>
        </div>
        <div id="data7" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class = "table">
              <tbody>
                <tr>
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
  <%@ include file="footer.jsp" %>
</div>
</body>
</html>
