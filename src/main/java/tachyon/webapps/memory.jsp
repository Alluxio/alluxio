<%@ page isELIgnored ="false" %> 
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>  

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
      <ul class="nav nav-pills">
        <li><a href="./home">Master: ${masterNodeAddress}</a></li>
        <li><a href="./browse?path=/">Browse File System</a></li>
        <li class="active"><a href="./memory">View Files in Memory</a></li>
      </ul>
    </div>
  </div>
  
  <div class="container-fluid">
    <div class="row-fluid">
      <div class="span12 well">
        <table class="table table-hover">
          <caption>Files Currently In Memory</caption>
          <tbody>
            <c:forEach var="file" items="${inMemoryFiles}">
              <th>${file}</th>
            </c:forEach>
          </tbody>
        </table>
      </div>
    </div>
  </div>
  <footer>
    <p>Tachyon is a project developed at the UC Berkeley <a href="https://amplab.cs.berkeley.edu">AMPLab</a>.</p>
  </footer>
</div>
</body>
</html>