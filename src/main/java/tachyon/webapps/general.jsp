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
						<li class="active"><a href="./home">Master Node XXXXX</a></li>
						<li><a href="./browse?path=/">Browse File System</a></li>
					</ul>
				</div>
			</div>
			<div class ="row-fluid">
			<div class="accordion span6" id="accordion1">
				<div class="accordion-group">
					<div class="accordion-heading">
						<a class="accordion-toggle" data-toggle="collapse" 
							data-parent="#accordion1" href="#data1">
							<h4>Instance Summary Here</h4>
						</a>
					</div>
					<div id="data1" class="accordion-body collapse in">
						<div class="accordion-inner">
							<table class="table">
								<tbody>
								<tr>
									<th>Started on:</th>
									<th>${startTime}</th>
								</tr>
								<tr>
									<th>Has been up for:</th>
									<th>${uptime}</th>
								</tr>
								<tr>
									<th>Running Version:</th>
									<th>$VARIABLEHERE</th>
								</tr>
								<tr>
									<th>Other Data:</th>
									<th>$VARIABLEHERE</th>
								</tr>
								</tbody>
							</table>
						</div>
					</div>
				</div>
			</div>
			
			<div class="accordion span6" id="accordion2">
				<div class="accordion-group">
					<div class="accordion-heading">
						<a class="accordion-toggle" data-toggle="collapse" 
							data-parent="#accordion2" href="#data2">
							<h4>Cluster Summary Here</h4>
						</a>
					</div>
					<div id="data2" class="accordion-body collapse in">
						<div class="accordion-inner">
							<table class="table">
								<tbody>
									<tr>
										<th>Configured Storage Capacity:</th>
										<th>$VARIABLEHERE</th>
									</tr>
									<tr>
										<th>Storage In-Use</th>
										<th>$VARIABLEHERE</th>
									</tr>
									<tr>
										<th>Slaves Running</th>
										<th>$VARIABLEHERE</th>
									</tr>
									<tr>
										<th>Decomissioning Nodes</th>
										<th>$VARIABLEHERE</th>
									</tr>
								</tbody>
							</table>
						</div>
					</div>
				</div>
			</div>	
			</div>
			<div class="row-fluid">

			<div class="accordion span14" id="accordion3">
				<div class="accordion-group">
					<div class="accordion-heading">
						<a class="accordion-toggle" data-toggle="collapse" 
							data-parent="#accordion3" href="#data3">
							<h4>Detailed Node Summary Here</h4>
						</a>
					</div>
					<div id="data3" class="accordion-body collapse in">
						<div class="accordion-inner">
							<table class="table table-hover">
								<thead>
									<th>Node Name</th>
									<th>Last Heartbeat</th>
									<th>State</th>
									<th>Blocks</th>	
									<th>Capacity</th>
								<tbody>
									<tr>
										<th>$VARIABLEHERE</th>
										<th>$VARIABLEHERE</th>
										<th>$VARIABLEHERE</th>
										<th>$VARIABLEHERE</th>
										<th>
											<div class="progress">
  												<div class="bar bar-success" style="width: 80%;">$%Free</div>
  												<div class="bar bar-danger" style="width: 20%;"></div>
											</div>
										</th>
									</tr>
									<tr>
										<th>$VARIABLEHERE</th>
										<th>$VARIABLEHERE</th>
										<th>$VARIABLEHERE</th>
										<th>$VARIABLEHERE</th>
										<th>
											<div class="progress">
  												<div class="bar bar-success" style="width: 80%;">$%Free</div>
  												<div class="bar bar-danger" style="width: 20%;"></div>
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
		</div>
	</body>
</html>