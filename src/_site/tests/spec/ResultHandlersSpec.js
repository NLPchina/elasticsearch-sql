/*
	============ QUERIES ===============

	simpleQueryResult:
		SELECT * FROM elasticsearch-sql_test_index


	simpleAggregationResult:
		SELECT count(*) AS count, sum(balance) AS balance_sum,
		avg(age) AS averge_age
		FROM elasticsearch-sql_test_index/account
		group by gender


  nestedAggregationResult:
    SELECT COUNT(*) AS count, AVG(balance) AS averge_balance,
    SUM(balance) AS sum_balance
    FROM elasticsearch-sql_test_index
    WHERE age > 27 AND age < 30
    GROUP BY gender, age
*/



function isSetsEquals(arr1, arr2) {
	if(arr1.length != arr2.length) {
		return false;
	}

	for(var i = 0; i < arr1.length; i++) {
		if(arr1[i] != arr2[i]) {
			return false;
		}
	}
	return true;
}


describe("ResultHandlerFactory", function() {
  
  var queryResult;
  var aggregationResult;

  beforeAll(function() {
  	jasmine.getJSONFixtures().fixturesPath = 'resources';
  	queryResult = getJSONFixture('simpleQueryResult.json');
  	aggregationResult = getJSONFixture('simpleAggregationResult.json');
  });

  it("should return DefaultQueryResultHandler", function() {
  	var handler = ResultHandlerFactory.create(queryResult)
    expect(handler.constructor).toBe(DefaultQueryResultHandler);
  });

  it("should return AggregationQueryResultHandler", function() {
  	var handler = ResultHandlerFactory.create(aggregationResult)
    expect(handler.constructor).toBe(AggregationQueryResultHandler);
  });
});



describe("DefaultQueryResultHandler", function() {
  
  var queryResult;  
  var expectedBody;
  var handler;

  beforeAll(function() {
  	jasmine.getJSONFixtures().fixturesPath = 'resources';
  	queryResult = getJSONFixture('simpleQueryResult.json');
  	expectedBody = getJSONFixture('expectedBody4simpleQueryResult.json');	
  	handler = new DefaultQueryResultHandler(queryResult);
  });

  it("should return the expected head", function() {
  	var expectedHead = ["account_number", "balance", "firstname", "lastname", "age", "gender", "address", "employer", "email", "city", "state"];
  	
    var head = handler.getHead();
    expect(isSetsEquals(expectedHead, head)).toBe(true);
  });

  it("should return the expected body", function() {  	
    var body = handler.getBody();
    expect(angular.equals(body, expectedBody)).toBe(true);
  });

});



describe("AggregationQueryResultHandler", function() {
  
  var simpleAggregationResult;
  var expectedBody4simpleAggregation;

  var nestedAggregationResult;
  var expectedBody4nestedAggregation;

  var statsAggregationResult;
  var expectedBody4statsAggregation;

  beforeAll(function() {
  	jasmine.getJSONFixtures().fixturesPath = 'resources';

  	simpleAggregationResult = getJSONFixture('simpleAggregationResult.json');
  	expectedBody4simpleAggregation = getJSONFixture('expectedBody4simpleAggregation.json');

    nestedAggregationResult = getJSONFixture('nestedAggregationResult.json');
    expectedBody4nestedAggregation = getJSONFixture('expectedBody4nestedAggregation.json');

    statsAggregationResult = getJSONFixture('statsAggResult.json');
    expectedBody4statsAggregation = getJSONFixture('expectedBody4statsAgg.json');
  });

  it("should return the expected head for simple aggregation", function() {
    var handler = new AggregationQueryResultHandler(simpleAggregationResult);
  	var expectedHead = ["gender", "averge_age", "count", "balance_sum"];
  	
    var head = handler.getHead();
    expect(isSetsEquals(expectedHead, head)).toBe(true);
  });


  it("should return the expected body for simple aggregation", function() {  
    var handler = new AggregationQueryResultHandler(simpleAggregationResult);
    var body = handler.getBody();
    expect(angular.equals(body, expectedBody4simpleAggregation)).toBe(true);
  });

  it("should return the expected head for nested aggregation", function() {
    var handler = new AggregationQueryResultHandler(nestedAggregationResult);
    var expectedHead = ["gender", "age", "count", "sum_balance", "averge_balance"];
    
    var head = handler.getHead();
    expect(isSetsEquals(expectedHead, head)).toBe(true);
  });

  it("should return the expected body for nested aggregation", function() {  
    var handler = new AggregationQueryResultHandler(nestedAggregationResult);
    var body = handler.getBody();
    expect(angular.equals(body, expectedBody4nestedAggregation)).toBe(true);
  });

  it("should return the expected head for stats aggregation", function() {
    var handler = new AggregationQueryResultHandler(statsAggregationResult);
    var expectedHead = ["stat.count", "stat.min", "stat.max", "stat.avg","stat.sum"];
    
    var head = handler.getHead();
    expect(isSetsEquals(expectedHead, head)).toBe(true);
  });

  it("should return the expected body for stats aggregation", function() {  
    var handler = new AggregationQueryResultHandler(statsAggregationResult);
    var body = handler.getBody();
    expect(angular.equals(body, expectedBody4statsAggregation)).toBe(true);
  });

});