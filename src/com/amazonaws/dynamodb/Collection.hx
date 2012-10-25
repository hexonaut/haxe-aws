package com.amazonaws.dynamodb;

/**
 * Represents a collection of attributes from a given table.
 * @author Sam MacPherson
 */

import com.amazonaws.dynamodb.Database;

class Collection {
	
	var items:Array<Hash<Dynamic>>;
	public var consumedCapacityUnits(default, null):Float;

	public function new (items:Array<Hash<Dynamic>>, consumedCapacityUnits:Float) {
		this.items = items;
		this.consumedCapacityUnits = consumedCapacityUnits;
	}
	
	public function iterator ():Iterable<Hash<Dynamic>> {
		return items;
	}
	
	/**
	 * Collection based operations may commonly stop for many reasons.
	 * This value tells you if you have the entire set available.
	 * 
	 * @return true if the collection is complete.
	 */
	public function completed ():Bool {
		return true;
	}
	
}

class BatchedCollection extends Collection {
	
	var unprocessedKeys:Array<PrimaryKey>;
	var attributesToGet:Array<String>;
	
	public function new (items:Array<Hash<Dynamic>>, consumedCapacityUnits:Float, ?unprocessedKeys:Array<PrimaryKey>, ?attributesToGet:Array<String>) {
		super(items, consumedCapacityUnits);
		
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