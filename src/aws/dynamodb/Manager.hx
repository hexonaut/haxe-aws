package aws.dynamodb;

import aws.dynamodb.DynamoDBError;
import aws.dynamodb.DynamoDBException;
import aws.dynamodb.RecordInfos;

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
	
	function haxeToDynamo (v:Dynamic):Dynamic {
		return switch (Type.typeof(v)) {
			case TFloat, TInt: { N:Std.string(v) };
			case TBool: { N:v == true ? "1" : "0" };
			case TClass(c):
				switch (Type.getClassName(c)) {
					case "String": { S:v };
					case "Date": { N:Std.string(cast(v, Date).getTime()) };
					default: throw "Unsupported type for DynamoDB '" + Type.getClassName(c) + "'.";
				}
			default: throw "Unsupported type for DynamoDB '" + Type.typeof(v) + "'.";
		}
	}
	
	function dynamoToHaxe (name:String, v:Dynamic):Dynamic {
		var infos = getInfos();
		
		for (i in Reflect.fields(v)) {
			var val = Reflect.field(v, i);
			for (o in infos.fields) {
				if (o.name == name) {
					return switch (o.type) {
						case DFloat: Std.parseFloat(v);
						case DInt: Std.parseInt(v);
						case DBool: v == "1";
						case DDate: Date.fromTime(Std.parseFloat(v));
						case DString: v;
					};
				}
			}
		}
		
		throw "Unknown DynamoDB type.";
	}
	
	function buildSpodObject (item:Dynamic):T {
		var infos = getInfos();
		
		var spod = Type.createEmptyInstance(cls);
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
	
	public function unsafeGet (id:Dynamic, ?consistent:Bool = true):T {
		var infos = getInfos();
		var keys:Dynamic = { };
		Reflect.setField(keys, infos.primaryIndex.hash.name, id);
		return unsafeGetWithKeys(keys, consistent);
	}
	
	public function unsafeGetWithKeys (keys:Dynamic, ?consistent:Bool = true):T {
		var dynkeys:Dynamic = { };
		for (i in Reflect.fields(keys)) {
			Reflect.setField(dynkeys, i, haxeToDynamo(Reflect.field(keys, i)));
		}
		return buildSpodObject(cnx.sendRequest("GetItem", {
			TableName: getTableName(),
			ConsistentRead: consistent,
			Key: dynkeys
		}).Item);
	}
	
	public function unsafeObjects (query:Dynamic, ?consistent:Bool = true):List<T> {
		Reflect.setField(query, "TableName", getTableName());
		Reflect.setField(query, "ConsistentRead", consistent);
		return Lambda.map(cast(cnx.sendRequest("Query", query).Items, Array<Dynamic>), function (e) { return buildSpodObject(e); } );
	}
	
	function buildRecordIndex (spod:T, index:RecordIndex):Dynamic {
		var obj:Dynamic = { };
		
		var hash = Reflect.field(spod, index.hash.name);
		if (hash == null) throw "Missing hash.";
		Reflect.setField(obj, index.hash.name, haxeToDynamo(hash));
		if (index.range != null) {
			var range = Reflect.field(spod, index.range.name);
			if (range == null) throw "Missing range.";
			Reflect.setField(obj, index.range.name, haxeToDynamo(range));
		}
		
		return obj;
	}
	
	public function doInsert (obj:T):Void {
		var infos = getInfos();
		
		var fields:Dynamic = { };
		for (i in infos.fields) {
			if (i.name != infos.primaryIndex.hash.name && i.name != infos.primaryIndex.range.name) {
				Reflect.setField(fields, i.name, {
					Action: "PUT",
					Value: haxeToDynamo(Reflect.field(obj, i.name))
				});
			}
		}
		
		cnx.sendRequest("UpdateItem", {
			TableName: getTableName(),
			Key: buildRecordIndex(obj, infos.primaryIndex),
			AttributeUpdates: fields
		});
	}
	
	public function objectToString (obj:T):String {
		return Std.string(obj);
	}
	#end
	
}