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
	hash:String,
	?range:String
}

enum RecordType {
	DString;
	DFloat;
	DInt;
	DBool;
	DBinary;
	DDate;
	DDateTime;
	DTimeStamp;
	DEnum(e:Enum<Dynamic>);
	DSet(type:RecordType);
}