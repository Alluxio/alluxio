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
  <div class="navbar navbar-inverse">
    <div class="navbar-inner">
      <ul class="nav nav-pills" style="height:40px;font-size:14px;font-weight: bold;vertical-align: bottom;">
        <li><a href="http://tachyon-project.org/" target="_blank"><img style="height:25px;margin-top:-5px;" src="img/logo.png" alt="Tachyon Logo"/></a></li>
        <li><a href="./home">Overview</a></li>
        <li><a href="./configuration">System Configuration</a></li>
        <li><a href="./browse?path=/">Browse File System</a></li>
        <li class="active"><a href="./memory?pageNum=1">In Memory Files</a></li>
      </ul>
    </div>
  </div>

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

        <% int lastPageNum = ((Integer)request.getAttribute("lastPageNum")).intValue(); %>
        <% if (lastPageNum > 0) { %>
          <% int currentPageNum = ((Integer)request.getAttribute("currentPageNum")).intValue(); %>
          <% int firstPageNum = ((Integer)request.getAttribute("firstPageNum")).intValue(); %>
          <% int paginationShow = ((Integer)request.getAttribute("paginationShow")).intValue(); %>
          <div class="pagination pagination-large pagination-centered">
            <ul>
              <li><a href='./memory?pageNum=1'>First</a></li>

              <% int prevPageNum = currentPageNum - 1; %>
              <% if (prevPageNum < 1) { %>
                <li class='disabled'><a href='./memory?pageNum=1'>Prev</a></li>
              <% } else { %>
                <li><a href='./memory?pageNum=<%= prevPageNum %>'>Prev</a></li>
              <% } %>

              <% for (int i = 1; i <= paginationShow; i ++) { %>
                <% int thisPageNum = firstPageNum - 1 + i; %>
                <% if (thisPageNum == currentPageNum) { %>
                  <li class='active'><a href='./memory?pageNum=<%= thisPageNum %>'><%= thisPageNum %></a></li>
                <% } else if (thisPageNum > lastPageNum) { %>
                  <li class='disabled'><a href='./memory?pageNum=<%= thisPageNum %>'><%= thisPageNum %></a></li>
                <% } else { %>
                  <li><a href='./memory?pageNum=<%= thisPageNum %>'><%= thisPageNum %></a></li>
                <% } %>
              <% } %>

              <% int nextPageNum = currentPageNum + 1; %>
              <% if (nextPageNum > lastPageNum) { %>
                <li class='disabled'><a href='./memory?pageNum=<%= lastPageNum %>'>Next</a></li>
              <% } else { %>
                <li><a href='./memory?pageNum=<%= nextPageNum %>'>Next</a></li>
              <% } %>

              <li><a href="./memory?pageNum=<%= lastPageNum %>">Last</a></li>
            </ul>
          </div>
        <% } %>
      </div>
    </div>
  </div>
  <footer>
    <p style="text-align: center;">
      <a href="http://tachyon-project.org/" target="_blank">Tachyon</a> is an <a href="https://github.com/amplab/tachyon" target="_blank">open source</a> project developed at the UC Berkeley <a href="https://amplab.cs.berkeley.edu" target="_blank">AMPLab</a>.
    </p>
  </footer>
</div>
<script>
// REALLY disable bootstrap's disabled <li> in pagination
$('.pagination .disabled a').on('click', function(e) {
    e.preventDefault();
});
</script>
</body>
</html>
