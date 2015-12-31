package aws.dynamodb;

typedef RecordInfos = {
	?prefix:String,
	table:String,
	?shard:String,
	primaryIndex:RecordIndex,
	indexes:Array<{ name:String, index:RecordIndex, global:Bool, ?readCap:Int, ?writeCap:Int }>,
	fields:Array<{ name:String, type:RecordType }>,
	relations:Array<RecordRelation>,
	?readCap:Int,
	?writeCap:Int
};

typedef RecordIndex = {
	hash:String,
	?range:String
}

typedef RecordRelation = {
	prop:String,
	key:String
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
	DStringEnum(e:Enum<Dynamic>);
	DSet(type:RecordType);
	DDeltaFloat;
	DDeltaInt;
	DUniqueSet(type:RecordType);
	DData;
}