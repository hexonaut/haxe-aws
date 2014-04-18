package aws.dynamodb;

import aws.dynamodb.DynamoDBError;
import aws.dynamodb.DynamoDBException;
import aws.dynamodb.RecordInfos;

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
			case TClass(c):
				switch (Type.getClassName(c)) {
					case "String": { S:v };
					default: throw "Unsupported type for DynamoDB.";
				}
			default: throw "Unsupported type for DynamoDB.";
		}
	}
	
	function dynamoToHaxe (v:Dynamic):Dynamic {
		for (i in Reflect.fields(v)) {
			switch (i) {
				case "S": return Reflect.field(v, i);
				case "N": return Std.parseFloat(Reflect.field(v, i));
			}
		}
		
		throw "Unknown DynamoDB type.";
	}
	
	function buildSpodObject (item:Dynamic):T {
		var spod = Type.createEmptyInstance(cls);
		for (i in Reflect.fields(item)) {
			Reflect.setField(spod, i, dynamoToHaxe(Reflect.field(item, i)));
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
	#end
	
}