package aws.dynamodb;

import aws.dynamodb.DynamoDBError;
import aws.dynamodb.DynamoDBException;
import aws.dynamodb.RecordInfos;
import haxe.crypto.Base64;

using Lambda;

class Manager<T:sys.db.Object> {
	
	static inline var SERVICE:String = "DynamoDB";
	static inline var API_VERSION:String = "20120810";
	
	#if !macro
	public static var cnx:Connection;
	#end
	
	var cls:Class<T>;

	public function new (cls:Class<T>) {
		this.cls = cls;
	}
	
	public macro function get (ethis, id, ?consistent:haxe.macro.Expr.ExprOf<Bool>): #if macro haxe.macro.Expr #else haxe.macro.Expr.ExprOf<T> #end {
		return RecordMacros.macroGet(ethis, id, consistent);
	}
	
	public macro function search (ethis, cond, ?options, ?consistent:haxe.macro.Expr.ExprOf<Bool>): #if macro haxe.macro.Expr #else haxe.macro.Expr.ExprOf<List<T>> #end {
		return RecordMacros.macroSearch(ethis, cond, options, consistent);
	}
	
	public macro function select (ethis, cond, ?options, ?consistent:haxe.macro.Expr.ExprOf<Bool>): #if macro haxe.macro.Expr #else haxe.macro.Expr.ExprOf<List<T>> #end {
		return RecordMacros.macroSearch(ethis, cond, options, consistent, true);
	}
	
	#if !macro
	function getInfos ():RecordInfos {
		return untyped cls.__dynamodb_infos;
	}
	
	function getFieldType (name:String):RecordType {
		var infos = getInfos();
		
		for (i in infos.fields) {
			if (i.name == name) {
				return i.type;
			}
		}
		
		return null;
	}
	
	function encodeVal (val:Dynamic, type:RecordType):{t:String, v:Dynamic} {
		return switch (type) {
			case DString: {t:"S", v:val};
			case DFloat, DInt: {t:"N", v:Std.string(val)};
			case DBool: {t:"N", v:(val ? "1" : "0")};
			case DDate:
				var date = cast(val, Date);
				date = new Date(date.getFullYear(), date.getMonth(), date.getDate(), 0, 0, 0);
				{t:"N", v:Std.string(date.getTime())};
			case DDateTime: {t:"N", v:Std.string(cast(val, Date).getTime())};
			case DTimeStamp:
				var t = cast(val, Date).getTime();
				//Add random precision if we need to
				if (Math.ffloor(t) == t) t += Math.random() * 1000;
				{t:"N", v:Std.string(t)};
			case DBinary: {t:"B", v:Base64.encode(val)};
			case DEnum(e): { t:"N", v:Std.string(val) };
			case DSet(t):
				var dtype = switch (t) {
					case DString: "SS";
					case DBinary: "BS";
					default: "NS";
				}
				var list = new Array<Dynamic>();
				for (i in cast(val, List<Dynamic>)) {
					list.push(encodeVal(i, t).v);
				}
				if (list.length == 0) throw "Set must contain at least one value.";
				{t:dtype, v:list};
		};
	}
	
	public function haxeToDynamo (name:String, v:Dynamic):Dynamic {
		var obj:Dynamic = { };
		var ev = encodeVal(v, getFieldType(name));
		
		Reflect.setField(obj, ev.t, ev.v);
		
		return obj;
	}
	
	function decodeVal (val:Dynamic, type:RecordType):Dynamic {
		return switch (type) {
			case DString: val;
			case DFloat: Std.parseFloat(val);
			case DInt: Std.parseInt(val);
			case DBool: val == "1";
			case DDate, DDateTime, DTimeStamp: Date.fromTime(Std.parseFloat(val));
			case DBinary: Base64.decode(val);
			case DEnum(e): Std.parseInt(val);
			case DSet(t):
				var list = new List<Dynamic>();
				for (i in cast(val, Array<Dynamic>)) {
					list.add(decodeVal(i, t));
				}
				list;
		};
	}
	
	public function dynamoToHaxe (name:String, v:Dynamic):Dynamic {
		var infos = getInfos();
		
		for (i in Reflect.fields(v)) {
			var val = Reflect.field(v, i);
			return decodeVal(val, getFieldType(name));
		}
		
		throw "Unknown DynamoDB type.";
	}
	
	function buildSpodObject (item:Dynamic):T {
		var infos = getInfos();
		
		var spod = Type.createInstance(cls, []);
		for (i in Reflect.fields(item)) {
			if (infos.fields.exists(function (e) { return e.name == i; } )) Reflect.setField(spod, i, dynamoToHaxe(i, Reflect.field(item, i)));
		}
		return spod;
	}
	
	function getTableName ():String {
		var infos = getInfos();
		var str = "";
		if (infos.prefix != null) {
			str += infos.prefix;
		}
		str += infos.table;
		if (infos.shard != null) {
			//Fill in temporal sharding with UTC time
			var now = Date.now();
			now = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0);
			now = DateTools.delta(Date.now(), now.getTime() - 24. * 3600 * 1000 * Math.round(now.getTime() / 24 / 3600 / 1000));
			str += DateTools.format(now, infos.shard);
		}
		return str;
	}
	
	public function unsafeGet (id:Dynamic, ?consistent:Bool = false):T {
		var infos = getInfos();
		var keys:Dynamic = { };
		Reflect.setField(keys, infos.primaryIndex.hash, id);
		return unsafeGetWithKeys(keys, consistent);
	}
	
	public function unsafeGetWithKeys (keys:Dynamic, ?consistent:Bool = false):T {
		var dynkeys:Dynamic = { };
		for (i in Reflect.fields(keys)) {
			Reflect.setField(dynkeys, i, haxeToDynamo(i, Reflect.field(keys, i)));
		}
		return buildSpodObject(cnx.sendRequest("GetItem", {
			TableName: getTableName(),
			ConsistentRead: consistent,
			Key: dynkeys
		}).Item);
	}
	
	public function unsafeObjects (query:Dynamic, ?consistent:Bool = false):List<T> {
		Reflect.setField(query, "TableName", getTableName());
		Reflect.setField(query, "ConsistentRead", consistent);
		return Lambda.map(cast(cnx.sendRequest("Query", query).Items, Array<Dynamic>), function (e) { return buildSpodObject(e); } );
	}
	
	function checkKeyExists (spod:T, index:RecordIndex):Void {
		if (Reflect.field(spod, index.hash) == null) throw "Missing hash.";
		if (index.range != null) {
			if (Reflect.field(spod, index.range) == null) throw "Missing range.";
		}
	}
	
	function buildRecordExpected (spod:T, index:RecordIndex, exists:Bool):Dynamic {
		var obj:Dynamic = { };
		
		var hash = { Exists:exists };
		if (exists) Reflect.setField(hash, "Value", haxeToDynamo(index.hash, Reflect.field(spod, index.hash)));
		Reflect.setField(obj, index.hash, hash);
		if (index.range != null) {
			var range = { Exists:exists };
			if (exists) Reflect.setField(range, "Value", haxeToDynamo(index.range, Reflect.field(spod, index.range)));
			Reflect.setField(obj, index.range, range);
		}
		
		return obj;
	}
	
	function buildFields (spod:T):Dynamic {
		var infos = getInfos();
		var fields:Dynamic = { };
		
		for (i in infos.fields) {
			var v = Reflect.field(spod, i.name);
			if (v != null) {
				if (Std.is(v, String) && cast(v, String).length == 0) throw "String values must have length greater than 0.";
				
				Reflect.setField(fields, i.name, haxeToDynamo(i.name, v));
			}
		}
		
		return fields;
	}
	
	public function doInsert (obj:T):Void {
		var infos = getInfos();
		checkKeyExists(obj, infos.primaryIndex);
		
		cnx.sendRequest("PutItem ", {
			TableName: getTableName(),
			Expected: buildRecordExpected(obj, infos.primaryIndex, false),
			Item: buildFields(obj)
		});
	}
	
	public function doUpdate (obj:T):Void {
		var infos = getInfos();
		checkKeyExists(obj, infos.primaryIndex);
		
		cnx.sendRequest("PutItem ", {
			TableName: getTableName(),
			Expected: buildRecordExpected(obj, infos.primaryIndex, true),
			Item: buildFields(obj)
		});
	}
	
	public function doPut (obj:T):Void {
		var infos = getInfos();
		checkKeyExists(obj, infos.primaryIndex);
		
		cnx.sendRequest("PutItem ", {
			TableName: getTableName(),
			Item: buildFields(obj)
		});
	}
	
	public function doDelete (obj:T):Void {
		var infos = getInfos();
		checkKeyExists(obj, infos.primaryIndex);
		
		var key = { };
		Reflect.setField(key, infos.primaryIndex.hash, haxeToDynamo(infos.primaryIndex.hash, Reflect.field(obj, infos.primaryIndex.hash)));
		if (infos.primaryIndex.range != null) Reflect.setField(key, infos.primaryIndex.range, haxeToDynamo(infos.primaryIndex.range, Reflect.field(obj, infos.primaryIndex.range)));
		
		cnx.sendRequest("DeleteItem ", {
			TableName: getTableName(),
			Key: key
		});
	}
	
	public function objectToString (o:T):String {
		return Std.string(o);
	}
	#end
	
}