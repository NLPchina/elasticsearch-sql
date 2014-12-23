/*
	============ QUERIES ===============

	simpleQueryResult:
		SELECT * FROM elasticsearch-sql_test_index


	simpleAggregationResult:
		SELECT count(*) AS count, sum(balance) AS balance_sum,
		avg(age) AS averge_age
		FROM elasticsearch-sql_test_index/account
		group by gender
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
  
  var aggregationResult;  
  var expectedBody;
  var handler;

  beforeAll(function() {
  	jasmine.getJSONFixtures().fixturesPath = 'resources';
  	aggregationResult = getJSONFixture('simpleAggregationResult.json');
  	expectedBody = [
  		{"gender": "m", "averge_age": 30.027613412228796, "count": 507, "balance_sum": 13082527},
  		{"gender": "f", "averge_age": 30.3184584178499, "count": 493, "balance_sum": 12632310}  		
  	]
  	handler = new AggregationQueryResultHandler(aggregationResult);
  });

  it("should return the expected head", function() {
  	var expectedHead = ["gender", "averge_age", "count", "balance_sum"];
  	
    var head = handler.getHead();
    expect(isSetsEquals(expectedHead, head)).toBe(true);
  });


  /* TODO make this test pass.
  Add test that take complex aggregation result with
  some buckets. */
  it("should return the expected body", function() {  
    var body = handler.getBody();
    expect(angular.equals(body, expectedBody)).toBe(true);
  });

});