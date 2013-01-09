package com.amazonaws.dynamodb;

/**
 * Provides information about a table.
 * @author Sam MacPherson
 */

enum TableStatus {
	CREATING;
	ACTIVE;
	DELETING;
	UPDATING;
}

class TableInfo {
	
	public var name(default, null):String;
	public var created(default, null):Date;
	public var readCapacity(default, null):Int;
	public var writeCapacity(default, null):Int;
	public var lastDecrease(default, null):Null<Date>;
	public var lastIncrease(default, null):Null<Date>;
	public var size(default, null):Int;		//Table size in bytes
	public var status(default, null):TableStatus;
	
	public function new (?data:Dynamic) {
		if (data != null) {
			this.name = data.TableName;
			this.created = Date.fromTime(data.CreationDateTime * 1000);
			this.readCapacity = data.ProvisionedThroughput.ReadCapacityUnits;
			this.writeCapacity = data.ProvisionedThroughput.WriteCapacityUnits;
			this.lastDecrease = data.ProvisionedThroughput.LastDecreaseDateTime != null ? Date.fromTime(data.ProvisionedThroughput.LastDecreaseDateTime * 1000) : null;
			this.lastIncrease = data.ProvisionedThroughput.LastIncreaseDateTime != null ? Date.fromTime(data.ProvisionedThroughput.LastIncreaseDateTime * 1000) : null;
			this.size = data.TableSizeBytes;
			this.status = switch (data.TableStatus) {
				case "CREATING": CREATING;
				case "ACTIVE": ACTIVE;
				case "DELETING": DELETING;
				case "UPDATING": UPDATING;
			}
		}
	}
	
}