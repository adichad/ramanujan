'use strict';

var ramanujanDBCtrl = function($scope,$http) {
   $scope.cols = [];
   $scope.dbtables = [];

  $scope.add_row = function() {
    $scope.cols.push({});
  };

  $scope.delete_row = function(row) {
    $scope.cols.splice(row, 1);
  };
  $scope.fetchTables=function(){
    var value = document.getElementById("alias").value
    console.log("[DEBUG] Value == "+value)
    console.log("[DEBUG] [REQUEST] fetchtables was called , inside ramanujanDBCtrl . . .")
    $http.get("web/static/tables_"+value+".php").then(function (response) { $scope.dbtables = Array.from(response.data.tables); console.log($scope.dbtables)})
    /* thought I would add table fetch and then column fetch code :-) */
  }
  $scope.fetchColumns=function(){
    var value = document.getElementById("table").value
    console.log("[DEBUG] [REQUEST] fetchColumns was called , inside ramanujanDBCtrl . . .")
    /* thought I would add table fetch and then column fetch code :-) */
  }
  $scope.sendTable=function(){
      /* while compiling form , angular created this object*/
      console.log("[DEBUG] [REQUEST] sendTable inside ramanujanDBCtrl was called == Requests . . .")
      var data=$scope.table;  
      if(data.alias === "crm"){
        data.port = "3306";
        data.password = "Az8Dg344";
        data.user = "investopad";
        data.host = "prod-crm-read5.curnxxtqpaar.ap-southeast-1.rds.amazonaws.com";
        data.conntype = "com.mysql.jdbc.Driver";
        data.db = "crmdb_prod";
      }
      else if(data.alias === "mpdm"){
        data.port = "3306";
        data.password = "Az8Dg344";
        data.user = "investopad";
        data.host = "mpdm-3-readreplica.curnxxtqpaar.ap-southeast-1.rds.amazonaws.com";
        data.conntype = "com.mysql.jdbc.Driver";
        data.db = "mpdm";
      }
      else if(data.alias === "ncrm"){
        data.port = "3306";
        data.password = "Az8Dg344";
        data.user = "investopad";
        data.host = "prod-ncrm-read.curnxxtqpaar.ap-southeast-1.rds.amazonaws.com";
        data.conntype = "com.mysql.jdbc.Driver";
        data.db = "ncrm_prod";
      }
      else if(data.alias === "amb"){
        data.port = "3308";
        data.password = "jyoti@123";
        data.user = "jyoti";
        data.host = "10.100.40.239";
        data.conntype = "com.mysql.jdbc.Driver";
        data.db = "live_amb_orders";
      }
      else{
        console.log("[DEBUG] [REQUEST] data alias == "+data.alias)
      }
      data.cols = $scope.cols;
      console.log("[DEBUG] [REQUEST] just dumping the data json in here == ")
      console.log(data)
      /* post to server*/
      console.log("[DEBUG] [REQUEST] posting the data to api request url . . .")
      $http.post('/api/request/rdbms', data).success(function() {console.log("[DEBUG] [REQUEST] great success ! "+Math.random())}).error(function(){console.log("[DEBUG] [REQUEST] uh ho! "+(1+Math.random()))});        
      console.log("[DEBUG] [REQUEST] posted !")
  }
}

var ramanujanLoginCtrl = function($scope,$http,$location) {
  $scope.login=function(){
      /* while compiling form , angular created this object*/
      console.log("[DEBUG] [REQUEST] sendTable inside ramanujanLoginCtrl was called == Requests . . .")
      var data=$scope.loginDetails;  
      console.log("[DEBUG] [REQUEST] just dumping the data json in here == ")
      console.log(data)
      /* post to server*/
      console.log("[DEBUG] [REQUEST] posting the data to api request url . . .")
      $http.post('/login', data).success(function() {console.log("[DEBUG] [REQUEST] great success ! "+Math.random());$location.path('/Ramanujan');}).error(function(){console.log("[DEBUG] [REQUEST] uh ho! "+(1+Math.random()))});        
      console.log("[DEBUG] [REQUEST] [LOGIN] posted !")
  }
}


var ramanujanKafkaCtrl = function($scope,$http) {
   $scope.kafkaCols = [];

  $scope.add_row_kafka = function() {
    $scope.kafkaCols.push({});
  };

  $scope.delete_row_kafka = function(row) {
    $scope.kafkaCols.splice(row, 1);
  };
  $scope.sendTopic=function(){
      /* while compiling form , angular created this object*/
      console.log("[DEBUG] [REQUEST] sendTable inside ramanujanKafkaCtrl was called == Requests . . .")
      var data=$scope.topic;  
      data.kafkaCols = $scope.kafkaCols;
      console.log("[DEBUG] [REQUEST] just dumping the data json in here == ")
      console.log(data)
      /* post to server*/
      console.log("[DEBUG] [REQUEST] posting the data to api request url . . .")
      $http.post('/api/request/kafka', data).success(function() {console.log("[DEBUG] [REQUEST] great success ! "+Math.random())}).error(function(){console.log("[DEBUG] [REQUEST] uh ho! "+(1+Math.random()))});        
      console.log("[DEBUG] [REQUEST] posted !")
  }
}

var reportCtrl = function($scope,$http) {
	/*
	$scope.reports = ["none","omniture_report","other_report"];
	$scope.report=$scope.reports[0]
	$scope.$watch('report', function(report) {
		for(var i in $scope.reports){
			var option = $scope.reports[i];
			if(option === report){
				$scope.report = option;
				console.log("[MY DEBUG STATEMENTS] the $scope.report now . . . == "+$scope.report)
				break;
			}
		}
		var data=$scope.report;
		console.log("[MY DEBUG STATEMENTS] dumping the data json == ")
		console.log(data)
		console.log("[MY DEBUG STATEMENTS] posting the data to api request url . . .")
		$http.post('http:/localhost:9999/api/report',data).then(function() {console.log("great success ! "+Math.random())}).catch(function(){console.log("uh ho! "+(1+Math.random()))});
	 });
	*/
	$scope.submitReport=function(){
		var script = $scope.script 
		console.log("[MY DEBUG STATEMENTS] [REPORT] the script obtained , would be dumped here . . .")
		console.log(script)
		/* post to server */
		console.log("[MY DEBUG STATEMENTS] [REPORT] posting the data to api request url . . .")
		$http.post('/api/report',script).then(function() {console.log("[MY DEBUG STATEMENTS] [REPORT] great success on report submission ! "+Math.random())},function(){console.log("[MY DEBUG STATEMENTS] [REPORT] uh ho for report submission ! "+(1+Math.random()))});
		console.log("[MY DEBUG STATEMENTS] [REPORT] posted !")
	}
}