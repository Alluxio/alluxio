<%@ page isELIgnored ="false" %> 
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link href="bootstrap/css/bootstrap.min.css" rel="stylesheet" media="screen">
</head>
<title>Tachyon</title>
<body>
<script src="http://code.jquery.com/jquery-latest.min.js" type="text/javascript"></script>
<script src="bootstrap/js/bootstrap.min.js"></script>
<div class="container-fluid">
	
	<div class="navbar navbar-inverse">
		<div class="navbar-inner">
			<ul class="nav nav-pills">
				<li><a href="./home">Master Node XXXXX</a></li>
				<li class="active"><a href="./browse?path=/">Browse File System</a></li>
			</ul>
		</div>
	</div>
	
	<div class="container-fluid">
		<div class="row-fluid">
			<div class="span2 well">
				<h3>Directory Listing</h3>
				<li>
					$VARIABLEHERE
				</li>
			</div>
			<div class="span10 well">
				<table class="table">
					<caption><h3>Directory Path: ${currentPath}</h3></caption>
					<thead>
						<th>File Name</th>
						<th>Size</th>
						<th>In-Memory</th>
					</thead>
					<tbody>
						<tr>
							<th><a href="#">$VARIABLEHERE</a></th>
							<th>$VARIABLEHERE</th>
							<th>$VARIABLEHERE</th>
						</tr>
						<tr>
							<th><a href="#">$VARIABLEHERE</a></th>
							<th>$VARIABLEHERE</th>
							<th>$VARIABLEHERE</th>
						</tr>
					</tbody>
				</table>
			</div>
		</div>
	</div>
	
	
	
	
</div>
</body>
</html>