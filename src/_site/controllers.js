var elasticsearchSqlApp = angular.module('elasticsearchSqlApp', ["ngAnimate", "ngSanitize"]);

elasticsearchSqlApp.controller('MainController', function ($scope, $http, $sce) {
	$scope.url = "http://localhost:9200";
	$scope.error = "";	
	$scope.resultsColumns = [];
	$scope.resultsRows = [];
	$scope.loading = false;

	$scope.search = function() {
		// Reset results and error box
		$scope.error = "";
		$scope.resultsColumns = [];
		$scope.resultsRows = [];
		$scope.loading = true;
		$scope.$apply();

        var query = window.editor.getValue();

		$http.post($scope.url + "/_sql", query)
		.success(function(data, status, headers, config) {
          var handler = ResultHandlerFactory.create(data);
          $scope.resultsColumns = handler.getHead();
          $scope.resultsRows = handler.getBody();
      
        })
        .error(function(data, status, headers, config) {        
          if(data == "") {
            $scope.error = "Error occured! response is not avalible.";
    	  }
    	  else {
    	  	$scope.error = JSON.stringify(data);
		  }
        })
        .finally(function() {
          $scope.loading = false;
          $scope.$apply()    
        });
	}

	$scope.getSearchButtonContent = function(isLoading) {
		var loadingContent = "<span class=\"glyphicon glyphicon-refresh glyphicon-refresh-animate\"></span> Loading...";
		var returnValue = isLoading ? loadingContent : "Search";
		return $sce.trustAsHtml(returnValue);
	} 
});