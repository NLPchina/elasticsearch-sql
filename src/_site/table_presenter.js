
var TablePresenter = function(tableId,tableZoneSelector) {
	this.tableId = tableId;
	this.tableSelector = "#"+tableId;
	this.tableZoneSelector = tableZoneSelector;
	this.oldTable = false;
	this.table = undefined;
}

TablePresenter.prototype.addRows = function(rows) {
	if(this.oldTable) return;
	this.table.rows.add(createRows(rows)).draw();
};



TablePresenter.prototype.createOrReplace = function(columns,rows) {
	if(this.oldTable) return;

	clearTableIfNeeded(this.table,this.tableZoneSelector,this.tableId);

	dataTablesColumns = createColumns(columns);
    
    dataTableRows = createRows(rows);

    this.table = $(this.tableSelector).DataTable(
    {
    	"aaData": dataTableRows,
    	"aoColumns": dataTablesColumns,
    	"destroy": true,
    	"scrollX": true,
    	"order": [],
    	"lengthMenu": [[10, 25, 50,100, -1], [10, 25, 50,100, "All"]]
    });
};

TablePresenter.prototype.destroy = function() {
	clearTableIfNeeded(this.table,this.tableZoneSelector,this.tableId);
};

TablePresenter.prototype.changeTableType = function(old,$compile,$scope){
	if(old) {
		createOldTable(this.tableZoneSelector,this.tableId ,$compile,$scope);
		this.oldTable = true;
	}
	else {
		this.oldTable = false;
	}
}


function clearTableIfNeeded(table,tableZoneSelector,tableId){
	if(table != undefined){
		table.clear();
		$(tableZoneSelector).empty();
		$(tableZoneSelector).html('<table id="'+tableId+'"></table>');
	}
}

function createColumns(columns){
	dataTablesColumns = [];

	for(i = 0 ; i < columns.length ; i++){
		var column = columns[i];
		dataTableColumn = {}
		dataTableColumn["data"] = column.replace(/\./g,"&");
		dataTableColumn["sTitle"] = column;
		dataTableColumn["defaultContent"]="";

		dataTableColumn["render"] = function(data,type,row){
				if(typeof(data)=="object")
					return JSON.stringify(data);
				else
					return data;
			}
		
        dataTablesColumns.push(dataTableColumn);
    }
    return dataTablesColumns;
}
function createRows(rows){
	dataTableRows = [];
    for(i =0;i<rows.length;i++){
    	var row = rows[i]
    	dataTableRow = {};
    	for(key in row){
    		dataTableRow[key.replace(/\./g,"&")] = row[key];
    	}
    	dataTableRows.push(dataTableRow);
    }
    return dataTableRows;
}

function createOldTable (tableZoneSelector,tableId,$compile,$scope) {
	$(tableZoneSelector).empty();
	var html = '<table class=\'table table-striped\' id=\''+tableId+'\'>';
	html =  html + '<thead><tr id=\'tableHead\'><th ng-repeat=\'column in resultsColumns\'>{{column}}</th></tr>';
	html += '    </thead>   <tbody id=\'tableBody\'><tr ng-repeat=\'row in resultsRows\'><td ng-repeat=\'column in resultsColumns\'>{{row[column]}}</td></tr></tbody> </table>'
	 $(tableZoneSelector).html($compile(html)($scope));
                 
}
