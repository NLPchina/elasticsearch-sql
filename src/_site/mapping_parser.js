
var MappingParser = function(data) {

	var parsedMapping = parseMapping(data);
	this.mapping = parsedMapping;
}


function parseMapping(mapping){
	var indexToTypeToFields = {};
	for(index in mapping){
		var types = mapping[index]["mappings"];
		var typeToFields = {};
		for(type in types){
			var fields = types[type]["properties"];
			fieldsFlatten = {};
			getFieldsRecursive(fields,fieldsFlatten,"");
			typeToFields[type] = fieldsFlatten;
		}

		indexToTypeToFields[index] = typeToFields;
	}
	return indexToTypeToFields;	
}

function getFieldsRecursive(fields,fieldsFlatten,prefix){
	for(field in fields){
		var fieldMapping = fields[field];
		if("type" in fieldMapping){
			fieldsFlatten[prefix+field] = fieldMapping;
		}
		if(!("type" in fieldMapping) || fieldMapping.type == "nested") {
			getFieldsRecursive(fieldMapping["properties"],fieldsFlatten,prefix+field+".");
		}
	}
}

MappingParser.prototype.getIndices = function() {
  return Object.keys(this.mapping);
};

MappingParser.prototype.getTypes = function(index) {
  return Object.keys(this.mapping[index]);
};

MappingParser.prototype.getFieldsForType = function(index,type) {
  return Object.keys(this.mapping[index][type]);
};
MappingParser.prototype.getFieldsForTypeWithMapping = function(index,type) {
  return this.mapping[index][type];
};

