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
          <h1 class="text-error">
            <%= request.getAttribute("fatalError") %>
          </h1>
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

        <!-- pagination component -->
        <div class="pagination pagination-centered">
          <ul id="paginationUl">
          </ul>
        </div>

        <!-- view setting panel -->
        <form>
          <fieldset>
            <legend>View Settings</legend>
            <label>number of items on each page:</label>
            <input id="nFilePerPage" type="text" placeholder="default = 20">
            <label>maximum number of pages to show in pagination component:</label>
            <input id="nMaxPageShown" type="text" placeholder="default = 10">
          </fieldset>
        </form>
        <button class="btn" id="updateView">Update</button>
      </div>
    </div>
  </div>
  <%@ include file="footer.jsp" %>
</div>
<!-- where the magic behind dynamic pagination happens -->
<jsp:include page="memory-pagination.jsp" />
</body>
</html>
