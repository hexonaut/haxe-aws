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
 * Simplifies storing and retrieving objects from the database.
 * 
 * @author Sam MacPherson
 */

import aws.dynamodb.DynamoDB;
import haxe.io.Bytes;
import haxe.rtti.Meta;
import haxe.Serializer;
import haxe.Unserializer;
import Type;

#if !macro @:autoBuild(aws.dynamodb.PersistantObjectMacro.build()) #end
class PersistantObject {
	
	static var AUTO_RETRIES_UPPER_LIMIT:Int = 64;		//If request fails after 64 seconds of waiting then give up
	
	public static var DATABASE:DynamoDB = null;
	public static var TABLE_PREFIX:String = null;
	
	@ignore var __db:DynamoDB;
	@ignore var __table:String;
	@ignore var __hash:String;
	@ignore var __range:String;

	public function new (?dbObject:Dynamic, ?db:DynamoDB, ?table:String) {
		if (db != null) __db = db;
		else __db = DATABASE;
		
		var meta = Meta.getType(Type.getClass(this));
		if (meta == null) throw "Meta tags required.";
		if (table != null) __table = table;
		else if (TABLE_PREFIX != null) __table = TABLE_PREFIX;
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
		var meta = Meta.getFields(Type.getClass(this));
		var fieldMeta = Reflect.field(meta, field);
		if (fieldMeta != null) {
			return Reflect.hasField(fieldMeta, "ignore");
		} else {
			return true;
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
		var meta = Meta.getFields(Type.getClass(this));
		var ref:String = Reflect.field(meta, field).type[0];
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
				return untyped Date.fromTime(val);
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
		for (i in Reflect.fields(Meta.getFields(Type.getClass(this)))) {
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