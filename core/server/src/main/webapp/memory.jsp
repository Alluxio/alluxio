<%@ page import="java.util.*" %>
<%@ page import="alluxio.web.*" %>

<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link href="css/bootstrap.min.css" rel="stylesheet" media="screen">
  <link href="css/custom.min.css" rel="stylesheet">
  <link href="img/favicon.ico" rel="shortcut icon">
</head>
<title>Alluxio</title>
<body>
<jsp:include page="header-scripts.jsp" />
<div class="container-fluid">
  <jsp:include page="/header" />

  <div class="container-fluid">
    <div class="row-fluid">
      <div class="span12 well">
          <h1 class="text-error">
            <%= request.getAttribute("fatalError") %>
          </h1>
          <table class="table table-hover">
          <thead>
            <th>File Path</th>
            <th>Size</th>
            <th>Block Size</th>
            <th>Permission</th>
            <th>Owner</th>
            <th>Group</th>
            <th>Pin</th>
            <th>Creation Time</th>
            <th>Modification Time</th>
          </thead>
          <tbody>
            <% if (request.getAttribute("fileInfos") != null) { %>
              <% for (UIFileInfo fileInfo : ((List<UIFileInfo>) request.getAttribute("fileInfos"))) { %>
                <tr>
                  <th><%= fileInfo.getAbsolutePath() %></th>
                  <th><%= fileInfo.getSize() %></th>
                  <th><%= fileInfo.getBlockSizeBytes() %></th>
                  <th><%= fileInfo.getPermission() %></th>
                  <th><%= fileInfo.getUserName() %></th>
                  <th><%= fileInfo.getGroupName() %></th>
                  <th><%= (fileInfo.isPinned() ? "YES" : "NO") %></th>
                  <th><%= fileInfo.getCreationTime() %></th>
                  <th><%= fileInfo.getModificationTime() %></th>
                </tr>
              <% } %>
            <% } %>
          </tbody>
        </table>

        <%@ include file="pagination-component.jsp" %>

      </div>
    </div>
  </div>

  <%@ include file="footer.jsp" %>
</div>

<!-- where the magic behind dynamic pagination happens -->
<%@ include file="memory-pagination-header.jsp" %>
<%@ include file="pagination-control.jsp" %>

</body>
</html>
