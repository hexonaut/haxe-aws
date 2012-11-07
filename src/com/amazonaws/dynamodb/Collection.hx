package com.amazonaws.dynamodb;

/**
 * Loads in items from the database either via scan or query.
 * 
 * @author Sam MacPherson
 */

import com.amazonaws.dynamodb.Database;
import haxe.FastList;

class Collection {
	
	static var AUTO_RETRIES_UPPER_LIMIT:Int = 64;		//If request fails after 64 seconds of waiting then give up
	
	public var db(default, null):Database;
	public var table(default, null):String;
	public var hashKey(default, null):Null<Dynamic>;
	public var attributesToGet:Null<Array<String>>;
	public var rangeKeyComparisonFunction:Null<ComparisonFunction>;
	public var filters:Null<Hash<ComparisonFunction>>;
	public var limit:Int;
	public var scanLimit:Int;
	public var count:Bool;
	public var scanForward:Bool;
	public var consistantRead:Bool;
	public var auto:Bool;
	public var metrics(default, null): { consumedCapacityUnits:Int, count:Int, scannedCount:Int };
	
	var firstLoad:Bool;
	var head:Null<FastCell<Attributes>>;
	var tail:Null<FastCell<Attributes>>;
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
	public function new (db:Database, table:String, ?hashKey:Dynamic, ?options:{ ?limit:Int, ?scanLimit:Int, ?count:Bool, ?scanForward:Bool, ?consistantRead:Bool, ?auto:Bool }) {
		this.db = db;
		this.table = table;
		this.hashKey = hashKey;
		this.firstLoad = false;
		this.head = null;
		this.tail = null;
		this.counted = 0;
		this.delay = 1;
		this.metrics = { consumedCapacityUnits:0, count:0, scannedCount:0 };
		
		if (options == null) {
			//Defaults
			limit = 0;
			scanLimit = 0;
			count = false;
			scanForward = true;
			consistantRead = false;
			auto = true;
		} else {
			limit = options.limit != null ? options.limit : 0;
			scanLimit = options.scanLimit != null ? options.scanLimit : 0;
			count = options.count != null ? options.count : false;
			scanForward = options.scanForward != null ? options.scanForward : true;
			consistantRead = options.consistantRead != null ? options.consistantRead : false;
			auto = options.auto != null ? options.auto : true;
		}
	}
	
	function queryMoreItems ():Void {
		var result:QueryScanResult = null;
		if (auto) {
			//We are automatically handling errors and retrying queries with exponentially increasing delays
			while (result == null) {
				try {
					if (hashKey != null) result = db.query(table, hashKey, rangeKeyComparisonFunction, attributesToGet, limit, count, scanForward, consistantRead, lastEvaluatedKey);
					else result = db.scan(table, filters, attributesToGet, scanLimit, count, lastEvaluatedKey);
				} catch (e:DynamoDBError) {
					if (result == null) {
						if (delay > AUTO_RETRIES_UPPER_LIMIT) throw "Failed to retrieve items from database.";
						
						Sys.sleep(delay);
						delay = delay << 1;
					}
				}
			}
		} else {
			//Throw errors to the user
			if (hashKey != null) result = db.query(table, hashKey, rangeKeyComparisonFunction, attributesToGet, limit, count, scanForward, consistantRead, lastEvaluatedKey);
			else result = db.scan(table, filters, attributesToGet, scanLimit, count, lastEvaluatedKey);
		}
		
		//Add items to the list
		for (i in result.items) {
			var node = new FastCell<Attributes>(i, null);
			if (tail == null) {
				head = node;
				tail = node;
			} else {
				tail.next = node;
				tail = node;
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
		
		if (!firstLoad || (head == null && lastEvaluatedKey != null)) {
			//Load first batch
			queryMoreItems();
			firstLoad = true;
		}
		
		return head != null || lastEvaluatedKey != null;
	}
	
	public function next ():Attributes {
		if (head == null) return null;
		
		var item = head.elt;
		head = head.next;
		counted++;
		return item;
	}
	
}