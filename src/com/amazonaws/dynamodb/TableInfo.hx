/****
* Copyright (C) 2013 Sam MacPherson
* 
* Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
* 
* The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
****/

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