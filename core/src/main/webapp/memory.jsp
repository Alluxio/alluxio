<%@ page import="java.util.*" %>
<%@ page import="tachyon.web.*" %>

<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link href="css/bootstrap.min.css" rel="stylesheet" media="screen">
</head>
<title>Tachyon</title>
<body>
<script src="js/jquery-1.9.1.min.js" type="text/javascript"></script>
<script src="js/bootstrap.min.js"></script>
<div class="container-fluid">
  <jsp:include page="header.jsp" />

  <div class="container-fluid">
    <div class="row-fluid">
      <div class="span12 well">
        <table class="table table-hover">
          <caption>Files Currently In Memory</caption>
          <thead>
            <th>File Name</th>
            <th>Size</th>
            <th>Block Size</th>
            <th>Pin</th>
            <th>Creation Time</th>
            <th>Modification Time</th>
          </thead>
          <tbody>
            <% if (request.getAttribute("fileInfos") != null) { %>
              <% for (UiFileInfo fileInfo : ((List<UiFileInfo>) request.getAttribute("fileInfos"))) { %>
                <tr>
                  <th><%= fileInfo.getAbsolutePath() %></th>
                  <th><%= fileInfo.getSize() %></th>
                  <th><%= fileInfo.getBlockSizeBytes() %></th>
                  <th><%= (fileInfo.getNeedPin() ? "YES" : "NO") %></th>
                  <th><%= fileInfo.getCreationTime() %></th>
                  <th><%= fileInfo.getModificationTime() %></th>
                </tr>
              <% } %>
            <% } %>
          </tbody>
        </table>
      </div>
    </div>
  </div>
  <%@ include file="footer.jsp" %>
</div>
</body>
</html>
