package aws.dynamodb;

typedef RecordInfos = {
	?prefix:String,
	table:String,
	?shard:String,
	primaryIndex:RecordIndex,
	indexes:Array<{ name:String, index:RecordIndex }>,
	fields:Array<{ name:String, type:RecordType }>
};

typedef RecordIndex = {
	hash:{ name:String, type:RecordType },
	?range:{ name:String, type:RecordType }
}

enum RecordType {
	DString;
	DFloat;
	DInt;
	DBool;
	DDate;
}