
var elasticsearchSqlApp = angular.module('elasticsearchSqlApp', ["ngAnimate", "ngSanitize"]);

elasticsearchSqlApp.controller('MainController', function ($scope, $http, $sce) {
	scroll_url = "_search/scroll?scroll=1m&scroll_id=";
	$scope.url = getUrl();
	$scope.showResults = false;
	$scope.error = "";
	$scope.resultsColumns = [];
	$scope.resultsRows = [];
	$scope.searchLoading = false;
	$scope.explainLoading = false;
	$scope.nextLoading = false;
	$scope.resultExplan = false;
	$scope.scrollId = null;
	$scope.gotNext = false;
	$scope.delimiter = ',';
	$scope.amountDescription = "";
	var fetched = 0;
	var total = 0 ;
	// pull version and put it on the scope
    $http.get($scope.url).success(function (data) {
        $http.get($scope.url + "_nodes/" + data.name).success(function (nodeData) {
            var node = nodeData.nodes[Object.keys(nodeData.nodes)[0]];
            angular.forEach(node.plugins, function (plugin) {
                if (plugin.name === "sql") {
                    $scope.version = plugin.version;
                }
            });
        });
    });

    function searchTillEndAndExportCsv (scrollId) {
    	//todo: get total amount show fetched/total
    	head = []
    	body = []
    	$scope.showResults = true;
    	callScrollAndFillBodyTillEnd(scrollId,head,body,true);
    }
    function updateDescription (handler) {
    	total = handler.getTotal();
        fetched += handler.getCurrentHitsSize();
        $scope.amountDescription = fetched + "/" + total 
    }
    function callScrollAndFillBodyTillEnd (scrollId,head,body,firstTime) {
		var url = $scope.url + scroll_url + scrollId;
    	$http.get(url)
		.success(function(data, status, headers, config) {

          var handler = ResultHandlerFactory.create(data,$scope.isFlat);
          	
          	updateDescription(handler);
           recieved = handler.getBody()
          if(body.length > 0){
          	body = body.concat(recieved);
          	//todo: extend head?
          	head = handler.getHead();
          }
          else {
          	body = recieved;
            head = handler.getHead();

          }
          if(recieved == undefined || recieved.length == undefined || recieved.length == 0){
          	if(firstTime){
          	 callScrollAndFillBodyTillEnd(handler.getScrollId(),head,body,false);
          	}
          	else {
          		exportCSVWithoutScope(head,body);
          	}
          }
          else {
          	callScrollAndFillBodyTillEnd(handler.getScrollId(),head,body,false);
          }


        })
        .error(function(data, status, headers, config) {
          if(data == "") {
            $scope.error = "Error occured! response is not avalible.";
    	  }
    	  else {
    	  	$scope.error = JSON.stringify(data);
    	  	$scope.scrollId = undefined;
		  }
        })
        .finally(function() {
          $scope.nextLoading = false;
          $scope.$apply()
        });


    	// body...
    }

	$scope.nextSearch = function(){
		$scope.error = "";
		$scope.nextLoading = true;
		$scope.$apply();


		if($scope.scrollId == undefined || $scope.scrollId == "" ){
			$scope.error = "tryed scrolling with empty scrollId";
			return;
		}

		$http.get($scope.url + scroll_url + $scope.scrollId)
		.success(function(data, status, headers, config) {
          var handler = ResultHandlerFactory.create(data,$scope.isFlat);
          updateDescription(handler);
          var body = handler.getBody()

          if(body.length == undefined || body.length == 0){
          	$scope.gotNext=false;
          }
          else
          {
          	  $scope.scrollId = handler.getScrollId();
          }

          if($scope.resultsRows.length > 0){
          	$scope.resultsRows = $scope.resultsRows.concat(handler.getBody());
          }
          else {
          	$scope.resultsColumns = handler.getHead();
            $scope.resultsRows = handler.getBody();

          }


        })
        .error(function(data, status, headers, config) {
          if(data == "") {
            $scope.error = "Error occured! response is not avalible.";
    	  }
    	  else {
    	  	$scope.error = JSON.stringify(data);
    	  	$scope.scrollId = null;
		  }
        })
        .finally(function() {
          $scope.nextLoading = false;
          $scope.$apply()
        });

	}

	$scope.search = function() {
		// Reset results and error box
		$scope.error = "";
		fetched = 0;
		total = 0;
		$scope.amountDescription = 0;
		$scope.resultsColumns = [];
		$scope.resultsRows = [];
		$scope.searchLoading = true;
		$scope.$apply();
		$scope.resultExplan = false;

		saveUrl()

        var query = window.editor.getValue();

		$http.post($scope.url + "_sql", query)
		.success(function(data, status, headers, config) {
          var handler = ResultHandlerFactory.create(data,$scope.isFlat);
          updateDescription(handler);
          if(handler.isScroll){
          	
          	$scope.scrollId = handler.getScrollId();
          	if($scope.isAutoSave){
          		searchTillEndAndExportCsv($scope.scrollId);
          	}
          	else {
          		$scope.gotNext=true;
          	}
          }
          
          else {

          	if($scope.isAutoSave){
                $scope.showResults=true;
          		exportCSVWithoutScope(handler.getHead(),handler.getBody());
          	}
            else {
                $scope.resultsColumns = handler.getHead();
                $scope.resultsRows = handler.getBody();
            }
          }
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
          $scope.searchLoading = false;
          $scope.$apply()
        });
	}

	$scope.explain = function() {
		// Reset results and error box
		$scope.error = "";
		$scope.resultsColumns = [];
		$scope.resultsRows = [];
		$scope.explainLoading = true;
		$scope.$apply();
		$scope.resultExplan = true;

		saveUrl()

        var query = window.editor.getValue();
		$http.post($scope.url + "_sql/_explain", query)
		.success(function(data, status, headers, config) {
					 $scope.resultExplan = true;
				   window.explanResult.setValue(JSON.stringify(data, null, "\t"));
        })
        .error(function(data, status, headers, config) {
        	$scope.resultExplan = false;
          if(data == "") {
            $scope.error = "Error occured! response is not avalible.";
    	  }
    	  else {
    	  	$scope.error = JSON.stringify(data);
		  }
        })
        .finally(function() {
          $scope.explainLoading = false;
          $scope.$apply()
        });
	}


	 function exportCSVWithoutScope(columns,rows) {
		var delimiter = $scope.delimiter;
		var data =arr2csvStr(columns,delimiter) ;
		for(var i=0; i<rows.length ; i++){
		data += "\n";
			data += map2csvStr(columns,rows[i],delimiter) ;
		}
		var plain = 'data:text/csv;charset=utf8,' + encodeURIComponent(data);
		download(plain, "query_result.csv", "text/plain");
  		return true;
	}


	$scope.exportCSV = function() {
		var columns = $scope.resultsColumns ;
		var rows = $scope.resultsRows ;
		exportCSVWithoutScope(columns,rows);
	}

	$scope.getButtonContent = function(isLoading , defName) {
		var loadingContent = "<span class=\"glyphicon glyphicon-refresh glyphicon-refresh-animate\"></span> Loading...";
		var returnValue = isLoading ? loadingContent : defName;
		return $sce.trustAsHtml(returnValue);
	}


	function arr2csvStr(arr,op){
		var data = arr[0];
		for(var i=1; i<arr.length ; i++){
				data += op;
				data += arr[i] ;
		}
		return data ;
	}

	function map2csvStr(columns,arr,op){
		var data = JSON.stringify(arr[columns[0]]);
		for(var i=1; i<columns.length ; i++){
				data += op;
				data += JSON.stringify(arr[columns[i]]) ;
		}
		return data ;
	}


	function getUrl() {
		var url = localStorage.getItem("lasturl");
		if(url == undefined) {
			if(location.protocol == "file") {
				url = "http://localhost:9200"
			}
			else {
				url = location.protocol+'//' + location.hostname + (location.port ? ':'+location.port : '');
			}
		}

		if(url.substr(url.length - 1, 1) != '/') {
			url += '/'
		}

		return url
	}

	function saveUrl() {
		localStorage.setItem("lasturl", $scope.url);
	}
});