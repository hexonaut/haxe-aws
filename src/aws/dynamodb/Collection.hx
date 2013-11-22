/****
* Copyright (C) 2013 Sam MacPherson
* 
* Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
* 
* The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
****/

package aws.dynamodb;

/**
 * Loads in items from the database either via scan or query.
 * 
 * @author Sam MacPherson
 */

import aws.dynamodb.DynamoDB;

class Collection {
	
	static var AUTO_RETRIES_UPPER_LIMIT:Int = 64;		//If request fails after 64 seconds of waiting then give up
	
	public var db(default, null):DynamoDB;
	public var table(default, null):String;
	public var hashKey(default, null):Null<Dynamic>;
	public var attributesToGet:Null<Array<String>>;
	public var rangeKeyComparisonFunction:Null<ComparisonFunction>;
	public var filters:Null<Map<String, ComparisonFunction>>;
	public var limit:Int;
	public var scanLimit:Int;
	public var doCount:Bool;
	public var scanForward:Bool;
	public var consistantRead:Bool;
	public var auto:Bool;
	public var metrics(default, null): { consumedCapacityUnits:Int, count:Int, scannedCount:Int };
	
	var firstLoad:Bool;
	var head:Null<CollectionCell>;
	var tail:Null<CollectionCell>;
	var lastEvaluatedKey:Null<PrimaryKey>;
	var counted:Int;
	var delay:Int;
	
	/**
	 * Constructs an iterable collection to conveniently scan or query a table.
	 * 
	 * @param	db	A reference to the database.
	 * @param	table	The table you want to query or scan.
	 * @param	?hashKey	If provided then a query will be performed on this key. If not then the whole database will be scanned.
	 * @param	?options	Additional parameters.
	 */
	public function new (db:DynamoDB, table:String, ?hashKey:Dynamic, ?options:{ ?limit:Int, ?scanLimit:Int, ?scanForward:Bool, ?consistantRead:Bool, ?auto:Bool }, ?lastEvaluatedKey:PrimaryKey) {
		this.db = db;
		this.table = table;
		this.hashKey = hashKey;
		this.firstLoad = false;
		this.head = null;
		this.tail = null;
		this.counted = 0;
		this.doCount = false;
		this.lastEvaluatedKey = lastEvaluatedKey;
		this.delay = 1;
		this.metrics = { consumedCapacityUnits:0, count:0, scannedCount:0 };
		
		if (options == null) {
			//Defaults
			limit = 0;
			scanLimit = 0;
			scanForward = true;
			consistantRead = false;
			auto = true;
		} else {
			limit = options.limit != null ? options.limit : 0;
			scanLimit = options.scanLimit != null ? options.scanLimit : 0;
			scanForward = options.scanForward != null ? options.scanForward : true;
			consistantRead = options.consistantRead != null ? options.consistantRead : false;
			auto = options.auto != null ? options.auto : true;
		}
	}
	
	
	/**
	 * Constructs an iterable collection for querying a table.
	 * 
	 * @param	db	A reference to the database.
	 * @param	table	The table you want to query.
	 * @param	hashKey	The hash key to query on.
	 * @param	?options	Additional parameters.
	 * @return	A collection.
	 */
	public static function query (db:DynamoDB, table:String, hashKey:Dynamic, ?options:{ ?limit:Int, ?scanLimit:Int, ?scanForward:Bool, ?consistantRead:Bool, ?auto:Bool }, ?lastEvaluatedKey:PrimaryKey):Collection {
		return new Collection(db, table, hashKey, options, lastEvaluatedKey);
	}
	
	
	/**
	 * Constructs an iterable collection for scanning a table.
	 * 
	 * @param	db	A reference to the database.
	 * @param	table	The table you want to scan.
	 * @param	?options	Additional parameters.
	 * @return	A collection.
	 */
	public static function scan (db:DynamoDB, table:String, ?options:{ ?limit:Int, ?scanLimit:Int, ?scanForward:Bool, ?consistantRead:Bool, ?auto:Bool }, ?lastEvaluatedKey:PrimaryKey):Collection {
		return new Collection(db, table, null, options, lastEvaluatedKey);
	}
	
	function queryMoreItems ():Void {
		var result:QueryScanResult = null;
		if (auto) {
			//We are automatically handling errors and retrying queries with exponentially increasing delays
			delay = 1;
			while (result == null) {
				try {
					if (hashKey != null) result = db.query(table, hashKey, rangeKeyComparisonFunction, attributesToGet, limit, doCount, scanForward, consistantRead, lastEvaluatedKey);
					else result = db.scan(table, filters, attributesToGet, scanLimit, doCount, lastEvaluatedKey);
				} catch (e:DynamoDBException) {
					if (result == null) {
						if (delay > AUTO_RETRIES_UPPER_LIMIT) throw "Failed to retrieve items from database.";
						
						Sys.sleep(delay);
						delay = delay << 1;
					}
				}
			}
		} else {
			//Throw errors to the user
			if (hashKey != null) result = db.query(table, hashKey, rangeKeyComparisonFunction, attributesToGet, limit, doCount, scanForward, consistantRead, lastEvaluatedKey);
			else result = db.scan(table, filters, attributesToGet, scanLimit, doCount, lastEvaluatedKey);
		}
		
		//Add items to the list
		if (result.items != null) {
			for (i in result.items) {
				var node = new CollectionCell(i, null);
				if (tail == null) {
					head = node;
					tail = node;
				} else {
					tail.next = node;
					tail = node;
				}
			}
		}
		
		//Adjust metrics
		metrics.consumedCapacityUnits += result.consumedCapacityUnits;
		metrics.count += result.count;
		if (result.scannedCount != null) metrics.scannedCount += result.scannedCount;
		
		lastEvaluatedKey = result.lastEvaluatedKey;
	}
	
	public function hasNext ():Bool {
		//Limit has been reached
		if (limit != 0 && counted == limit) return false;
		
		while (!firstLoad || (head == null && lastEvaluatedKey != null)) {
			//Load first batch
			queryMoreItems();
			firstLoad = true;
		}
		
		return head != null || lastEvaluatedKey != null;
	}
	
	public function next ():Attributes {
		if (doCount) {
			//This is a count operation so increment counted based on count results
			counted = metrics.count;
			return null;
		}
		
		if (head == null) return null;
		
		var item = head.elt;
		head = head.next;
		counted++;
		return item;
	}
	
	public function count ():Int {
		doCount = true;
		//Load in all possible items
		for (i in this) {
		}
		return metrics.count > limit && limit != 0 ? limit : metrics.count;
	}
	
}

private class CollectionCell {
	
	public var elt:Attribute;
	public var next:CollectionCell;
	
	public function new (elt:Attribute, ?next:CollectionCell) {
		this.elt = elt;
		this.next = next;
	}
	
}