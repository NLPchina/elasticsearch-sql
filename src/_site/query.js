
/* ResultHandlerFactory
 Returns the right Result Handler depend
 on the results */
var ResultHandlerFactory = {
    "create": function(data,isFlat,showScore,showType,showId) {
        function isSearch(){
            return "hits" in data
        }
        // Is query is of aggregation type? (SQL group by)
        function isAggregation() {
            return "aggregations" in data
        }
        function isDelete(){
            return "_indices" in data
        }

        if(isSearch()){
            return isAggregation() ? new AggregationQueryResultHandler(data) : 
            new DefaultQueryResultHandler(data,isFlat,showScore,showType,showId)
        }

        if(isDelete()){
            return new DeleteQueryResultHandler(data);
        }
        return new ShowQueryResultHandler(data);

    }
}




/* DefaultQueryResultHandler object 
 Handle the query result,
 in case of regular query
 (Not aggregation)
 */
var DefaultQueryResultHandler = function(data,isFlat,showScore,showType,showId) {

    // createScheme by traverse hits field
    function createScheme() {
        var hits = data.hits.hits
        scheme = []
        for(index=0; index<hits.length; index++) {
            hit = hits[index]
            header = $.extend({},hit._source,hit.fields)
            if(isFlat){
                findKeysRecursive(scheme,header,"");
            }

            else {
                for(key in header) {

                    if(scheme.indexOf(key) == -1) {
                        scheme.push(key)
                    }
                }       
            }
            
        }
        if(showType){
            scheme.push("_type");
        }
        if(showScore){
            scheme.push("_score");
        }
        if(showScore){
            scheme.push("_id");
        }
        return scheme
    }
    

    this.data = data
    this.head = createScheme()
    this.isFlat = isFlat;
    this.showScore = showScore;
    this.showType = showType;
    this.showId = showId;
    this.scrollId = data["_scroll_id"];
    this.isScroll = this.scrollId!=undefined && this.scrollId!="";
};

DefaultQueryResultHandler.prototype.isScroll = function() {
    return this.isScroll;
};

DefaultQueryResultHandler.prototype.getScrollId = function() {
    return this.scrollId;
};


DefaultQueryResultHandler.prototype.getHead = function() {
    return this.head
};

DefaultQueryResultHandler.prototype.getBody = function() {
    var hits = this.data.hits.hits
    var body = []
    for(var i = 0; i < hits.length; i++) {
        var row = hits[i]._source;
        if("fields" in hits[i]){
            addFieldsToRow(row,hits[i])
        }
        if(this.isFlat){
            row = flatRow(this.head,row);
        }
        if(this.showType){
            row["_type"] = hits[i]._type
        }
        if(this.showScore){
            row["_score"] = hits[i]._score
        }
        if(this.showId){
            row["_id"] = hits[i]._id
        }
        body.push(row)
    }
    return body
};

DefaultQueryResultHandler.prototype.getTotal = function() {
    let total = this.data.hits.total;
    return angular.isObject(total) ? total.value : total;
};

DefaultQueryResultHandler.prototype.getCurrentHitsSize = function() {
    return this.data.hits.hits.length;
};

function findKeysRecursive (scheme,keys,prefix) {
        for(key in keys){
            if(typeof(keys[key])=="object" && (!(keys[key] instanceof Array))){
                findKeysRecursive(scheme,keys[key],prefix+key+".")
            }
            else {
                if(scheme.indexOf(prefix+key) == -1){
                    scheme.push(prefix+key);
                }
            }
        }
    }
function flatRow (keys,row) {
    var flattenRow = {}
    for( i = 0 ; i< keys.length ; i++ ){
        key = keys[i];
        splittedKey = key.split(".");
        var found = true;
        currentObj = row;
        for( j = 0 ; j < splittedKey.length ; j++){
            if(currentObj[splittedKey[j]]==undefined){
                found = false;
                break;
            }
            else {
                currentObj = currentObj[splittedKey[j]];
            }
        }
        if(found){
            flattenRow[key] = currentObj;
        }
    }
    return flattenRow;
}

function addFieldsToRow (row,hit) {
    for(field in hit.fields){
        fieldValue = hit.fields[field];
        if( fieldValue instanceof Array ){
            if(fieldValue.length > 1)
                row[field] = fieldValue;
            else row[field] = fieldValue[0];
        }
        else {
            row[field] = fieldValue;
        }
    }
}

function removeNestedAndFilters (aggs) {
    for(field in aggs)
    {
        if (field.endsWith("@NESTED") || field.endsWith("@FILTER") || field.endsWith("@NESTED_REVERSED") || field.endsWith("@CHILDREN")){
            delete aggs[field]["doc_count"];
            delete aggs[field]["key"];
            leftField = Object.keys(aggs[field])[0];
            aggs[leftField] = aggs[field][leftField];
            delete aggs[field];
            removeNestedAndFilters(aggs);
        }
        if(typeof(aggs[field])=="object"){
            removeNestedAndFilters(aggs[field]);
        }
    }
}
/* AggregationQueryResultHandler object 
 Handle the query result,
 in case of Aggregation query
 (SQL group by)
 */
var AggregationQueryResultHandler = function(data) {
    removeNestedAndFilters(data.aggregations);
    function getRows(bucketName, bucket, additionalColumns) {
        var rows = []

        var subBuckets = getSubBuckets(bucket)
        if(subBuckets.length > 0) {
            for(var i = 0; i < subBuckets.length; i++) {
                var subBucketName = subBuckets[i]["bucketName"];
                var subBucket = subBuckets[i]["bucket"];

                var newAdditionalColumns = {};
                // bucket without parents.
                if(bucketName != undefined) {
                    var newColumn = {};
                    newColumn[bucketName] = bucket.key;
                    newAdditionalColumns = $.extend(newColumn, additionalColumns);
                }

                var newRows = getRows(subBucketName, subBucket, newAdditionalColumns)
                $.merge(rows, newRows);
            }
        }

        else { //zhongshu-comment 没有子bucket了，就是最里面的那一层了
            var obj = $.extend({}, additionalColumns)
            if(bucketName != undefined) {
                if(bucketName != undefined) {
                    if("key_as_string" in bucket){
                        obj[bucketName] = bucket["key_as_string"] //zhongshu-comment 给字段取别名
                    }
                    else {
                        obj[bucketName] = bucket.key
                    }
                }
            }

            for(var field in bucket) {

                var bucketValue = bucket[field]
                if(bucketValue.buckets != undefined ){ //zhongshu-comment 如果还有子bucket的话，那就继续递归
                    var newRows = getRows(subBucketName, bucketValue, newAdditionalColumns);
                    $.merge(rows, newRows);
                    continue;
                }
                if(bucketValue.value != undefined) {
                    if("value_as_string" in bucket[field]){
                        obj[field] = bucketValue["value_as_string"]
                    }
                    else {
                        obj[field] = bucketValue.value
                    }
                }
                else {
                    if(typeof(bucketValue)=="object"){
                        /*subBuckets = getSubBuckets(bucketValue);
                        if(subBuckets.length >0){
                             var newRows = getRows(subBucketName, {"buckets":subBuckets}, newAdditionalColumns);
                            $.merge(rows, newRows);
                            continue;
                        }*/

                        
                           fillFieldsForSpecificAggregation(obj,bucketValue,field);
                        
                    }
                }
            }
            rows.push(obj)
        }

        return rows
    }

    //zhongshu-comment 递归
    function fillFieldsForSpecificAggregation(obj,value,field)
    {   

        for(key in value){
            if(key == "values"){
                fillFieldsForSpecificAggregation(obj,value[key],field);
            }
            else {
                obj[field+"." +key] = value[key];
            }
        }
        return;
    }

    //zhongshu-comment 递归
    function getSubBuckets(bucket) {
        var subBuckets = [];
        for(var field in bucket) {
            var buckets = bucket[field].buckets
            if(buckets != undefined) {
                for(var i = 0; i < buckets.length; i++) {
                    subBuckets.push({"bucketName": field, "bucket": buckets[i]})
                }
            }
            else {
                innerAgg = bucket[field]; //zhongshu-comment innerAgg这个变量是哪来的，貌似没声明，到时问问松哥
                for(var innerField in innerAgg){
                    if(typeof(innerAgg[innerField])=="object"){
                        innerBuckets = getSubBuckets(innerAgg[innerField]);
                        $.merge(subBuckets,innerBuckets);
                    }    
                }
            }
        }

        return subBuckets
    }


    this.data = data
    this.flattenBuckets = getRows(undefined, data.aggregations, {}) //zhongshu-comment 入口
};

AggregationQueryResultHandler.prototype.getHead = function() {
    head = []
    for(var i = 0; i < this.flattenBuckets.length; i++) {
        var keys = Object.keys(this.flattenBuckets[i])
        for(var j = 0; j < keys.length; j++) {
            if($.inArray(keys[j], head) == -1) {
                head.push(keys[j])
            }
        }
    }
    return head
};

AggregationQueryResultHandler.prototype.getBody = function() {
    return this.flattenBuckets
};


AggregationQueryResultHandler.prototype.getTotal = function() {
    return undefined;
};

AggregationQueryResultHandler.prototype.getCurrentHitsSize = function() {
  return this.flattenBuckets.length;
};




/* ShowQueryResultHandler object
 for showing mapping in some levels (cluster, index and types)
 */
var ShowQueryResultHandler = function(data) {

    var mappingParser = new MappingParser(data);
    var indices = mappingParser.getIndices();
    body = [];
    if(indices.length > 1){
        this.head = ["index","types"];
        for(indexOfIndex in indices){
            var indexToTypes = {};
            var index = indices[indexOfIndex]
            indexToTypes["index"] = index;
            indexToTypes["types"] = mappingParser.getTypes(index);
            body.push(indexToTypes);
        }
    }
    else {
        var index  = indices[0];
        var types = mappingParser.getTypes(index);
        if(types.length > 1) {
            this.head = ["type","fields"];
            for(typeIndex in types){
                var typeToFields = {};
                var type = types[typeIndex];
                typeToFields["type"] = type;
                typeToFields["fields"] = mappingParser.getFieldsForType(index,type);
                body.push(typeToFields)
            }
        }
        else {
            this.head = ["field","type"];
            anyFieldContainsMore = false;
            fieldsWithMapping = mappingParser.getFieldsForTypeWithMapping(index,types[0]);
            for(field in fieldsWithMapping){
                fieldRow = {};
                fieldMapping = fieldsWithMapping[field];
                fieldRow["field"] = field;
                fieldRow["type"] = fieldMapping["type"];
                delete fieldMapping["type"];
                if(!$.isEmptyObject(fieldMapping)){
                    anyFieldContainsMore = true;
                    fieldRow["more"] = fieldMapping;
                }
                body.push(fieldRow);
            }
            if(anyFieldContainsMore) this.head.push("more");

        }
    }

    this.body = body;

};


ShowQueryResultHandler.prototype.getHead = function() {
    return this.head
};

ShowQueryResultHandler.prototype.getBody = function() {
    return this.body;
};

ShowQueryResultHandler.prototype.getTotal = function() {
    return this.body.length;
};

ShowQueryResultHandler.prototype.getCurrentHitsSize = function() {
  return this.body.length;
};


/* DeleteQueryResultHandler object
 to show delete result status
 */
var DeleteQueryResultHandler = function(data) {
    this.head = ["index_deleted_from","shards_successful","shards_failed"];
    body = []
    deleteData = data["_indices"];
    for(index in deleteData){
        deleteStat = {};
        deleteStat["index_deleted_from"] = index;
        shardsData = deleteData[index]["_shards"];
        deleteStat["shards_successful"] = shardsData["successful"];
        deleteStat["shards_failed"] = shardsData["failed"];
        body.push(deleteStat);
    }
    this.body = body;

};


DeleteQueryResultHandler.prototype.getHead = function() {
    return this.head;
};

DeleteQueryResultHandler.prototype.getBody = function() {
    return this.body;
};

DeleteQueryResultHandler.prototype.getTotal = function() {
    return 1;
};

DeleteQueryResultHandler.prototype.getCurrentHitsSize = function() {
  return 1;
};

