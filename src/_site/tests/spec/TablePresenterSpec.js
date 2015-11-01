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
      if(arr1 == undefined && arr2 !=undefined)
        return false;
      if(arr1!= undefined && arr2 == undefined)
        return false;
     if(arr1.length != arr2.length) {
      return false;
    }

    for(var i = 0; i < arr1.length; i++) {
      if(typeof(arr1[i])=="object" ){
        if(!isObjectsEqual(arr1[i],arr2[i])) 
          return false;
      }
      else {
        if(arr1[i] != arr2[i]) {
        return false;
        }  
      }
      
   }
   return true;
 }

 function isObjectsEqual (obj1,obj2) {
  if(!isSetsEquals(Object.keys(obj1),Object.keys(obj2))) 
    return false;

  for(key in obj1){
    if(obj2[key]!=obj1[key])
      return false;
  }
  return true;
}

  function getColsFromDataTable (table) {
     dataTableCols = [];
    table.columns().iterator('column',
          function(context,index){ 
            dataTableCols.push( context.aoColumns[index].data);
          } 
    );
    return dataTableCols;
  }

  function getRowsFromDataTable (table) {
    var dataTableRows = []; 
    table.rows().iterator('row',
            function(context,index){
              dataTableRows.push(context.aoData[index]._aData); 
            }
    );
    return dataTableRows;
  }




describe("TablePresenter.createOrReplaceTable", function() {

  var presenter;
  var dataTable;
  var columns = ["account_number", "balance", "firstname"];
  var columns_new = ["account_number_new", "balance_new", "firstname"];

  var rows = [{"account_number":1,"balance":1000,"firstname":"person1"},
  {"account_number":2,"balance":2000,"firstname":"person2"}];
  var rows_new = [{"account_number_new":1,"balance_new":1000,"firstname":"person1"},
  {"account_number_new":2,"balance_new":2000,"firstname":"person2"}];
  var rows_newer = [{"account_number_new":3,"balance_new":1003,"firstname":"person3"},
  {"account_number_new":5,"balance_new":2005,"firstname":"person5"}];

  beforeAll(function() {
    //tableId,tableZoneSelector
    presenter = new TablePresenter("myTable","#myTableZone");
    presenter.createOrReplace(columns,rows);
    dataTable = $('#myTable').DataTable();
  });

  it("should contain the sent columns", function() {
    dataTableCols = getColsFromDataTable(dataTable);
    expect(isSetsEquals(columns,dataTableCols)).toBe(true);
  });

  it("should contain the sent rows", function() {  
    var dataTableRows = getRowsFromDataTable(dataTable);
    expect(rows.length == dataTableRows.length).toBe(true);
    expect(isSetsEquals(rows,dataTableRows)).toBe(true);
  });

  it("should replace columns", function() {
    presenter.createOrReplace(columns_new,rows_new)  
    dataTable = $('#myTable').DataTable();
    dataTableCols = getColsFromDataTable(dataTable);
    expect(isSetsEquals(columns_new,dataTableCols)).toBe(true);
  });

  it("should replace rows", function() {
    presenter.createOrReplace(columns_new,rows_newer)  
    dataTable = $('#myTable').DataTable();
    dataTableCols = getRowsFromDataTable(dataTable);
    expect(isSetsEquals(rows_newer,dataTableRows)).toBe(true);
  });


});


describe("TablePresenter.addRows", function() {

  var presenter;
  var dataTable;
  var columns = ["account_number", "balance", "firstname"];

  var rows = [{"account_number":1,"balance":1000,"firstname":"person1"},
  {"account_number":2,"balance":2000,"firstname":"person2"}];
  var additional_row = [{"account_number":3,"balance":3000,"firstname":"person3"}];
  var additional_rows = [{"account_number":4,"balance":4000,"firstname":"person4"},
  {"account_number":5,"balance":5000,"firstname":"person5"}];
 
  beforeAll(function() {
    //tableId,tableZoneSelector
    presenter = new TablePresenter("myTable","#myTableZone");
    presenter.createOrReplace(columns,rows);
    dataTable = $('#myTable').DataTable();
  });

  it("should add single row", function() {
    presenter.addRows(additional_row);
    expected_rows = [].concat(rows).concat(additional_row);
    new_rows = getRowsFromDataTable(dataTable);
    expect(isSetsEquals(expected_rows,new_rows)).toBe(true);
  });

  it("should add multiple rows", function() {  
    presenter.addRows(additional_rows);
    expected_rows = [].concat(rows).concat(additional_row).concat(additional_rows);
    new_rows = getRowsFromDataTable(dataTable);
    expect(isSetsEquals(expected_rows,new_rows)).toBe(true);
  });

});

describe("TablePresenter.destroy", function() {

  var presenter;
  var dataTable;
  var columns = ["account_number", "balance", "firstname"];

  var rows = [{"account_number":1,"balance":1000,"firstname":"person1"},
  {"account_number":2,"balance":2000,"firstname":"person2"}];
 
  beforeAll(function() {
    //tableId,tableZoneSelector
    presenter = new TablePresenter("myTable","#myTableZone");
    presenter.createOrReplace(columns,rows);
    dataTable = $('#myTable').DataTable();
  });

  it("should empty table div", function() {
    presenter.destroy();
    dataTableDivBody = $('#myTable').html();
    expect(dataTableDivBody).toEqual("");
  });

 

});