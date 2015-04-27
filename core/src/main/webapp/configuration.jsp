<%@ page import="java.util.*" %>

<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link href="css/bootstrap.min.css" rel="stylesheet" media="screen">
  <link href="css/tachyoncustom.min.css" rel="stylesheet">
</head>
<title>Tachyon</title>
<body>
<jsp:include page="header-scripts.jsp" />
<div class="container-fluid">
  <jsp:include page="header.jsp" />
  <div class="row-fluid">

  </div>
  <div class="row-fluid">
    <div class="accordion span14" id="accordion8">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion8" href="#data8">
            <h4>Tachyon Configuration</h4>
          </a>
        </div>
        <div id="data8" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class = "table">
              <tbody>
                <% for (Map.Entry<String, String> entry : (SortedSet<Map.Entry<String, String>>) request.getAttribute("configuration")) { %>
                  <tr>
                    <th><%= entry.getKey() %></th>
                    <th><%= entry.getValue() %></th>
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
