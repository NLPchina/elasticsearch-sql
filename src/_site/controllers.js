// settings
var settings = location.search.substring(1).split("&").reduce(function (r, p) {
    r[decodeURIComponent(p.split("=")[0])] = decodeURIComponent(p.split("=")[1]);
    return r;
}, {});

var elasticsearchSqlApp = angular.module('elasticsearchSqlApp', ["ngAnimate", "ngSanitize"]);

// auth
if (settings['username']) localStorage.setItem("auth", "Basic " + window.btoa(settings['username'] + ":" + settings['password']));
if (localStorage.getItem("auth")) {
    elasticsearchSqlApp.config(function ($httpProvider) {
        $httpProvider.interceptors.push(function () {
            return {
                request: function (config) {
                    config.headers['Authorization'] = localStorage.getItem("auth");
                    return config;
                }
            };
        });
    });
}

elasticsearchSqlApp.controller('MainController', function ($scope, $http, $sce,$compile) {
	scroll_url = "_search/scroll?scroll=1m&scroll_id=";
	$scope.url = getUrl();
	$scope.showResults = false;
	$scope.error = "";
	$scope.resultsColumns =[];
	$scope.resultsRows = [];
	$scope.searchLoading = false;
	$scope.explainLoading = false;
	$scope.nextLoading = false;
  $scope.fetchAllLoading = false;
	$scope.resultExplan = false;
  
	$scope.scrollId = undefined;
  $scope.amountDescription = "";
  var optionsKey = "essql-options-v1";
  var fetched = 0;
  var total = 0 ;
  //checkboxes
  $scope.gotNext = false;
  $scope.config = loadOptionsFromStorageOrDefault();
	
  var tablePresenter = new TablePresenter('searchResult','#searchResultZone');
  
  /* todo move to class - options handler */
  $scope.saveConfigToStorage = function () {
    localStorage.setItem(optionsKey,JSON.stringify($scope.config));
  }
  function loadOptionsFromStorageOrDefault () {
    var defaultOptions = {
      isFlat : false,
      showId : false,
      useOldTable : false,
      scrollSize : 10,
      alwaysScroll : false,
      isAutoSave : false,
      delimiter : ',',
      showScore : false,
      showType : false
    }
    if (typeof(Storage) !== "undefined") {
      options = localStorage.getItem(optionsKey);
      if(options!=undefined)
        return JSON.parse(options);
      else
        return defaultOptions;
    } 
  }

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

/* todo move to class fetch*/
$scope.fetchAll = function(){

  $scope.showResults = true;

  callScrollAndFillBodyTillEnd($scope.scrollId,$scope.resultsColumns,$scope.resultsRows,true,false,true);
}
	$scope.nextSearch = function(){
		$scope.error = "";
		$scope.nextLoading = true;
		$scope.$apply();
    var needToBuildTable = false;


		if($scope.scrollId == undefined || $scope.scrollId == "" ){
			$scope.error = "tryed scrolling with empty scrollId";
			return;
		}

		$http.get($scope.url + scroll_url + $scope.scrollId)
		.success(function(data, status, headers, config) {
          var handler = ResultHandlerFactory.create(data,$scope.config.isFlat,$scope.config.showScore,$scope.config.showType,$scope.config.showId);
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
            tablePresenter.addRows(handler.getBody());
          	$scope.resultsRows = $scope.resultsRows.concat(handler.getBody());
          }
          else {
          	$scope.resultsColumns = handler.getHead();
            $scope.resultsRows = handler.getBody();
            needToBuildTable = true; 
          }


        })
        .error(function(data, status, headers, config) {
          if(data == "") {
            $scope.error = "Error occured! response is not available.";
    	  }
    	  else {
    	  	$scope.error = JSON.stringify(data);
    	  	$scope.scrollId = undefined;
		  }
        })
        .finally(function() {
          $scope.nextLoading = false;
          $scope.$apply();
          if(needToBuildTable) {
            tablePresenter.createOrReplace($scope.resultsColumns,$scope.resultsRows);
          }

        });

	}

function updateWithScrollIfNeeded (query) {
  if(!$scope.config.alwaysScroll)
      return query;
  if(query.indexOf("USE_SCROLL")!=-1)
    return query;
  var scrollHint = "/*! USE_SCROLL("+$scope.config.scrollSize+","+120000+") */";
  if(query.indexOf("select") !=-1)
    return query.replace("select","select " + scrollHint);
  if(query.indexOf("SELECT") !=-1)
    return query.replace("SELECT","select " + scrollHint);
  return query;
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
		
		$scope.resultExplan = false;
    tablePresenter.destroy();
    $scope.$apply();
		saveUrl()

    var query = window.editor.getValue();
    var selectedQuery = window.editor.getSelection();
    if(selectedQuery != "" && selectedQuery != undefined){
      query = selectedQuery;
    }

    query = updateWithScrollIfNeeded(query);
		$http.post($scope.url + "_nlpcn/sql", query)
		.success(function(data, status, headers, config) {
          var handler = ResultHandlerFactory.create(data,$scope.config.isFlat,$scope.config.showScore,$scope.config.showType,$scope.config.showId);
          updateDescription(handler);
          if(handler.isScroll){
          	$scope.showResults=true;
          	$scope.scrollId = handler.getScrollId();
          	if($scope.config.isAutoSave){
          		searchTillEndAndExportCsv($scope.scrollId);
          	}
          	else {
          		$scope.gotNext=true;
                scrollBudy = handler.getBody();
                //not using scan type
                if(scrollBudy.length >0 ){
                    $scope.resultsColumns = handler.getHead();
                    $scope.resultsRows = scrollBudy;
                }
          	}
          }
          
          else {

          	if($scope.config.isAutoSave){
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
            $scope.error = "Error occured! response is not available.";
    	  }
    	  else {
    	  	$scope.error = JSON.stringify(data);
		  }
        })
        .finally(function() {
          $scope.searchLoading = false;
          $scope.$apply();
          if($scope.resultsColumns.length >0){
            tablePresenter.createOrReplace($scope.resultsColumns,$scope.resultsRows);
          }
          
        });
	}

	$scope.explain = function() {
		// Reset results and error box
		$scope.error = "";
		$scope.resultsColumns = [];
		$scope.resultsRows = [];
		$scope.explainLoading = true;
        $scope.showResults = false;
        tablePresenter.destroy();
        $scope.resultExplan = true;
		$scope.$apply();


		saveUrl()

        var query = window.editor.getValue();
		$http.post($scope.url + "_nlpcn/sql/explain", query)
		.success(function(data, status, headers, config) {
					 $scope.resultExplan = true;
				   window.explanResult.setValue(JSON.stringify(data, null, "\t"));
        })
        .error(function(data, status, headers, config) {
        	$scope.resultExplan = false;
          if(data == "") {
            $scope.error = "Error occured! response is not available.";
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
		var delimiter = $scope.config.delimiter;
		var data =arr2csvStr(columns,delimiter) ;
		for(var i=0; i<rows.length ; i++){
		data += "\n";
			data += map2csvStr(columns,rows[i],delimiter) ;
		}
		var plain = 'data:text/csv;charset=utf8,' + encodeURIComponent(data);
		download(plain, "query_result.csv", "text/plain");
  		return true;
	}

      $scope.onChangeTablePresnterType = function(){
      //value = ?
      value = $scope.config.useOldTable;
      tablePresenter.destroy();
      tablePresenter.changeTableType(value,$compile,$scope);
    }
   
    function searchTillEndAndExportCsv (scrollId) {
      //todo: get total amount show fetched/total
      head = []
      body = []
      $scope.showResults = true;
      callScrollAndFillBodyTillEnd(scrollId,head,body,true,true,false);
    }
    function updateDescription (handler) {
        total = handler.getTotal();
        fetched += handler.getCurrentHitsSize();
        if(total == undefined){
          $scope.amountDescription = fetched  
        }
        else {
          if(total == fetched){
            $scope.gotNext = false;
          }
          $scope.amountDescription = fetched + "/" + total   
        }
    }
    function callScrollAndFillBodyTillEnd (scrollId,head,body,firstTime,needToExport,updatePresenter) {
    var url = $scope.url + scroll_url + scrollId;
      $http.get(url)
    .success(function(data, status, headers, config) {

          var handler = ResultHandlerFactory.create(data,$scope.isFlat);
            
            updateDescription(handler);
           recieved = handler.getBody()
          if(body.length > 0){
            body = body.concat(recieved);

            head = $.extend(head,handler.getHead());
            if(updatePresenter){
              tablePresenter.addRows(recieved);
            }
          }
          else {
            body = recieved;
            head = handler.getHead();
            if(updatePresenter){
             tablePresenter.createOrReplace(head,body);
            }
          }
          if(recieved == undefined || recieved.length == undefined || recieved.length == 0){
            if(firstTime){
             callScrollAndFillBodyTillEnd(handler.getScrollId(),head,body,false,needToExport,updatePresenter);
            }
            else {
              if(needToExport){
                exportCSVWithoutScope(head,body);
              }
            }
          }
          else {
            callScrollAndFillBodyTillEnd(handler.getScrollId(),head,body,false,needToExport,updatePresenter);
          }


        })
        .error(function(data, status, headers, config) {
          if(data == "") {
            $scope.error = "Error occured! response is not available.";
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
        var url = settings['base_uri'] || localStorage.getItem("lasturl");
		if(url == undefined) {
            if (location.protocol == "file") {
				url = "http://localhost:9200"
			}
			else {
				url = location.protocol+'//' + location.hostname + (location.port ? ':'+location.port : '');
			}
		}

        if (url.indexOf('http://') !== 0 && url.indexOf('https://') !== 0) {
            url = 'http://' + url;
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
