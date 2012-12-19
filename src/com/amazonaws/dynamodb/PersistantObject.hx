package com.amazonaws.dynamodb;

/**
 * Simplifies storing and retrieving objects from the database.
 * 
 * @author Sam MacPherson
 */

import com.amazonaws.dynamodb.Database;
import haxe.io.Bytes;
import haxe.rtti.Meta;
import haxe.Serializer;
import haxe.Unserializer;
import Type;

#if !macro @:autoBuild(com.amazonaws.dynamodb.PersistantObjectMacro.build()) #end
class PersistantObject {
	
	static var AUTO_RETRIES_UPPER_LIMIT:Int = 64;		//If request fails after 64 seconds of waiting then give up
	
	public static var DATABASE:Database = null;
	public static var TABLE_PREFIX:String = null;
	
	@ignore var __db:Database;
	@ignore var __table:String;
	@ignore var __hash:String;
	@ignore var __range:String;

	public function new (?dbObject:Dynamic, ?db:Database, ?table:String) {
		if (db != null) __db = db;
		else __db = DATABASE;
		
		var meta = untyped Type.getClass(this).__meta__.obj;
		if (meta == null) throw "Meta tags required.";
		if (TABLE_PREFIX != null) __table = TABLE_PREFIX;
		else __table = "";
		if (meta.table != null) __table += meta.table[0];
		if (meta.hash != null) __hash = meta.hash[0];
		if (meta.range != null) __range = meta.range[0];
		
		if (__hash == null) throw "Hash meta tag required.";
		if (__table == null) throw "Table meta tag required.";
		if (__db == null) throw "Database needs to be set.";
		
		if (dbObject != null) build(dbObject);
	}
	
	inline function __key ():PrimaryKey {
		var hash = Reflect.field(this, __hash);
		if (hash == null) throw "Primary key hash must not be null.";
		var key = { hash:__haxeToDb(Reflect.field(this, __hash)) };
		if (__range != null) {
			var range = Reflect.field(this, __range);
			if (range == null) throw "Primary key range must not be null.";
			Reflect.setField(key, "range", __haxeToDb(range));
		}
		return key;
	}
	
	inline function __shouldIgnore (field:String):Bool {
		var meta = { };
		var subMeta = untyped Type.getClass(this).__meta__.fields;
		for (i in Reflect.fields(subMeta)) {
			Reflect.setField(meta, i, Reflect.field(subMeta, i));
		}
		var superMeta = untyped PersistantObject.__meta__.fields;
		for (i in Reflect.fields(superMeta)) {
			Reflect.setField(meta, i, Reflect.field(superMeta, i));
		}
		var fieldMeta = Reflect.field(meta, field);
		if (fieldMeta != null) {
			return Reflect.hasField(fieldMeta, "ignore");
		} else {
			return false;
		}
	}
	
	inline function __haxeToDb (val:Dynamic):Dynamic {
		if (Std.is(val, Int) || Std.is(val, Float) || Std.is(val, String) || Std.is(val, Bytes)) {
			//These are all fine as is
			return val;
		} else if (Std.is(val, Bool)) {
			//Bool -> Int
			return val ? 1 : 0;
		} else if (Std.is(val, Date)) {
			//Date -> Float
			return cast(val, Date).getTime();
		} else {
			throw "Unsupported type.";
		}
	}
	
	inline function __dbToHaxe (val:Dynamic, field:String):Dynamic {
		var ref:String = Reflect.field(untyped Type.getClass(this).__meta__.fields, field).type[0];
		if (Std.is(val, String) || Std.is(val, Bytes)) {
			//These are all fine as is
			return val;
		} else if (Std.is(val, Int)) {
			//Int may be either an Int or a Bool
			if (ref == "Bool") {
				return val == 1;
			} else {
				return val;
			}
		} else if (Std.is(val, Float)) {
			//Float may be either a Float or a Date
			if (ref == "Date") {
				return Date.fromTime(val);
			} else {
				return val;
			}
		} else {
			throw "Unsupported type.";
		}
	}
	
	function __doOperation (method:String, args:Array<Dynamic>):Dynamic {
		var delay = 1;
		while (true) {
			try {
				return Reflect.callMethod(__db, Reflect.field(__db, method), args);
			} catch (e:DynamoDBException) {
				if (delay > AUTO_RETRIES_UPPER_LIMIT) throw "Failed to retrieve items from database.";
				
				Sys.sleep(delay);
				delay = delay << 1;
			}
		}
		return null;
	}
	
	public function build (dbObject:Dynamic):Void {
		for (i in Reflect.fields(untyped Type.getClass(this).prototype)) {
			var val = Reflect.field(dbObject, i);
			if (!__shouldIgnore(i) && val != null) Reflect.setField(this, i, __dbToHaxe(val, i));
		}
	}
	
	public function get ():Void {
		var item = __doOperation("getItem", [__table, __key(), null, false]);
		build(item);
	}
	
	public function insert ():Void {
		var item = { };
		for (i in Reflect.fields(this)) {
			if (!__shouldIgnore(i)) Reflect.setField(item, i, __haxeToDb(Reflect.field(this, i)));
		}
		var item = __doOperation("putItem", [__table, item, null, false]);
	}
	
	public function update ():Void {
		var item = new UpdateAttributes();
		for (i in Reflect.fields(this)) {
			if (!__shouldIgnore(i) && i != __hash && i != __range) item.set(i, {value:__haxeToDb(Reflect.field(this, i))});
		}
		var item = __doOperation("updateItem", [__table, __key(), item, null, null, false]);
	}
	
	public function delete ():Void {
		__doOperation("deleteItem", [__table, __key(), null, false]);
	}
	
}