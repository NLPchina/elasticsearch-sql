
/* ResultHandlerFactory
Returns the right Result Handler depend
on the results */
var ResultHandlerFactory = {
  "create": function(data) {

    // Is query is of aggregation type? (SQL group by)
    function isAggregation() {
      return "aggregations" in data
    }

    return isAggregation() ? new AggregationQueryResultHandler(data) : new DefaultQueryResultHandler(data)
  }
}




/* DefaultQueryResultHandler object 
Handle the query result,
in case of regular query
(Not aggregation)
*/
var DefaultQueryResultHandler = function(data) {

  // createScheme by traverse hits field
  function createScheme() {
    var hits = data.hits.hits
    scheme = []
    for(index=0; index<hits.length; index++) {
      hit = hits[index]

      for(key in hit._source) {
        if(scheme.indexOf(key) == -1) {
          scheme.push(key)
        }
      }
    }
    return scheme
  }

  this.data = data
  this.head = createScheme()
};

DefaultQueryResultHandler.prototype.getHead = function() {
  return this.head
};

DefaultQueryResultHandler.prototype.getBody = function() {
  var hits = this.data.hits.hits
  var body = []
  for(var i = 0; i < hits.length; i++) {
    body.push(hits[i]._source)
  }
  return body
};






/* AggregationQueryResultHandler object 
Handle the query result,
in case of Aggregation query
(SQL group by)
*/
var AggregationQueryResultHandler = function(data) {

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

    else {
      var obj = $.extend({}, additionalColumns)
      if(bucketName != undefined) {
        obj[bucketName] = bucket.key
      }

      for(var field in bucket) {              
        if(bucket[field].value != undefined) {
          obj[field] = bucket[field].value
        }
        else {
          continue;
        }
      }
      rows.push(obj)
    }

    return rows
  }


  function getSubBuckets(bucket) {
    var subBuckets = [];
    for(var field in bucket) {
      var buckets = bucket[field].buckets
      if(buckets != undefined) {
        for(var i = 0; i < buckets.length; i++) {
          subBuckets.push({"bucketName": field, "bucket": buckets[i]})
        }
      }
    }

    return subBuckets
  }
  

  this.data = data
  this.flattenBuckets = getRows(undefined, data.aggregations, {})
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


