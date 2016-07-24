'use strict';

var ramanujanCtrl = function($scope,$http) {

  $scope.sendTable=function(){
      /* while compiling form , angular created this object*/
      console.log("[DEBUG] [REQUEST] sendTable inside ramanujanCtrl was called == Requests . . .")
      var data=$scope.table;  
      console.log("[DEBUG] [REQUEST] just dumping the data json in here == ")
      console.log(data)
      /* post to server*/
      console.log("[DEBUG] [REQUEST] posting the data to api request url . . .")
      $http.post('http://127.0.0.1:9999/api/request', data).success(function() {console.log("[DEBUG] [REQUEST] great success ! "+Math.random())}).error(function(){console.log("[DEBUG] [REQUEST] uh ho! "+(1+Math.random()))});        
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
		$http.post('http:/127.0.0.1:9999/api/report',script).then(function() {console.log("[MY DEBUG STATEMENTS] [REPORT] great success on report submission ! "+Math.random())},function(){console.log("[MY DEBUG STATEMENTS] [REPORT] uh ho for report submission ! "+(1+Math.random()))});
		console.log("[MY DEBUG STATEMENTS] [REPORT] posted !")
	}
}