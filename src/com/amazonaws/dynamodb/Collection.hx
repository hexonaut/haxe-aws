package com.amazonaws.dynamodb;

/**
 * Represents a collection of attributes from a given table. Provides convenience methods for incomplete requests.
 * 
 * @author Sam MacPherson
 */

import com.amazonaws.dynamodb.Database;

class Collection {
	
	var db:Database;
	var items:Array<Attributes>;
	public var consumedCapacityUnits(default, null):Float;

	public function new (db:Database, items:Array<Attributes>, consumedCapacityUnits:Float) {
		this.db = db;
		this.items = items;
		this.consumedCapacityUnits = consumedCapacityUnits;
	}
	
	public function iterator ():Iterator<Attributes> {
		return items.iterator();
	}
	
	/**
	 * Collection based operations may commonly stop for many reasons.
	 * This value tells you if you have the entire set available.
	 * BatchGetItem operations will return true when all items have been fetched.
	 * Query/Scan operations will return true when the limit has been reached.
	 * 
	 * @return	true if the collection is complete.
	 */
	public function completed ():Bool {
		throw "Implementation for completed required.";
		return false;
	}
	
	/**
	 * Get the count of the number of returned items.
	 * 
	 * @return	The number of items returned from this operation.
	 */
	public function count ():Int {
		throw "Count not applicable for this operation.";
		return 0;
	}
	
	/**
	 * Get the count of the number of scanned items.
	 * 
	 * @return	The number of scanned items from this operation.
	 */
	public function scannedCount ():Int {
		throw "Scanned count not applicable for this operation.";
		return 0;
	}
	
	/**
	 * Get the upper limit supplied for this operation.
	 * 
	 * @return	The limit of the number of requested items. Will return 0 if all results are requested.
	 */
	public function limit ():Int {
		throw "Limit not applicable for this operation.";
		return 0;
	}
	
	/**
	 * Load more results into the collection. Loading will start from where it last left off and stop when the limit has been reached.
	 */
	public function loadNext ():Void {
		throw "Implementation for loadNext required.";
	}
	
	function append (items:Array<Attributes>, consumedCapacityUnits:Float):Void {
		this.items.concat(items);
		consumedCapacityUnits += consumedCapacityUnits;
	}
	
}

class BatchedCollection extends Collection {
	
	var unprocessedKeys:Array<PrimaryKey>;
	var attributesToGet:Array<String>;
	
	public function new (db:Database, items:Array<Attributes>, consumedCapacityUnits:Float, ?unprocessedKeys:Array<PrimaryKey>, ?attributesToGet:Array<String>) {
		super(db, items, consumedCapacityUnits);
		
		setUnprocessedKeys(unprocessedKeys, attributesToGet);
	}
	
	public function setUnprocessedKeys (unprocessedKeys:Array<PrimaryKey>, ?attributesToGet:Array<String>):Void {
		this.unprocessedKeys = unprocessedKeys;
		this.attributesToGet = attributesToGet;
	}
	
	public override function completed ():Bool {
		return unprocessedKeys == null;
	}
	
}

class QueryScanCollection extends Collection {
	
	public var table:String;
	public var lastEvaluatedKey:PrimaryKey;
	public var isScan:Bool;
	public var _count:Int;
	public var _scannedCount:Int;
	public var _limit:Int;
	public var useScannedLimit:Bool;
	public var attributesToGet:Array<String>;
	public var hashKey:Dynamic;
	public var comparisonFunction:ComparisonFunction;
	public var filters:Hash<ComparisonFunction>;
	public var scanForward:Bool;
	public var consistantRead:Bool;
	public var countRequest:Bool;
	
	public function new (db:Database, items:Array<Attributes>, consumedCapacityUnits:Float, table:String, isScan:Bool, count:Int, ?lastEvaluatedKey:PrimaryKey) {
		super(db, items, consumedCapacityUnits);
		
		this.table = table;
		this.isScan = isScan;
		this._count = count;
		this._limit = 0;
		this.useScannedLimit = false;
		this.lastEvaluatedKey = lastEvaluatedKey;
	}
	
	public override function completed ():Bool {
		if (lastEvaluatedKey == null) return true;
		else if (_limit != 0) return useScannedLimit ? (scannedCount == limit) : (count == limit);
		else return false;
	}
	
	public override function count ():Int {
		return _count;
	}
	
	public override function scannedCount ():Int {
		return _scannedCount;
	}
	
	public override function limit ():Int {
		return _limit;
	}
	
	public override function loadNext ():Void {
		var coll:QueryScanCollection;
		if (isScan) {
			coll = cast db.scan(table, filters, attributesToGet, useScannedLimit ? (_limit - _scannedCount) : (_limit - _count), useScannedLimit, countRequest, lastEvaluatedKey);
		} else {
			coll = cast db.query(table, hashKey, comparisonFunction, attributesToGet, _limit - _count, countRequest, scanForward);
			_scannedCount += coll._scannedCount;
		}
		_count += coll._count;
		lastEvaluatedKey = coll.lastEvaluatedKey;
		append(coll.items, coll.consumedCapacityUnits);
	}
	
}