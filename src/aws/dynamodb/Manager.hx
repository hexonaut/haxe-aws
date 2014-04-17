package aws.dynamodb;

import aws.dynamodb.DynamoDBError;
import aws.dynamodb.DynamoDBException;

typedef DynamoDBInfos = {
	?prefix:String,
	table:String,
	?shard:String,
	hash:String,
	?range:String
};

class Manager<T:sys.db.Object> {
	
	static inline var SERVICE:String = "DynamoDB";
	static inline var API_VERSION:String = "20120810";
	
	public static var config:DynamoDBConfig;
	
	var cls:Class<T>;

	public function new (cls:Class<T>) {
		this.cls = cls;
	}
	
	public macro function get (ethis, id, ?consistent:haxe.macro.Expr.ExprOf<Bool>): #if macro haxe.macro.Expr #else haxe.macro.Expr.ExprOf<T> #end {
		return switch (id.expr) {
			case EObjectDecl(_): macro $ethis.unsafeGetWithKeys($id, $consistent);
			default: macro $ethis.unsafeGet($id, $consistent);
		}
	}
	
	#if !macro
	function getInfos ():DynamoDBInfos {
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
			str += DateTools.format(Date.now(), infos.shard);
		}
		return str;
	}
	
	public function unsafeGet (id:Dynamic, ?consistent:Bool = true):T {
		var infos = getInfos();
		var keys:Dynamic = { };
		Reflect.setField(keys, infos.hash, id);
		return unsafeGetWithKeys(keys, consistent);
	}
	
	public function unsafeGetWithKeys (keys:Dynamic, ?consistent:Bool = true):T {
		var dynkeys:Dynamic = { };
		for (i in Reflect.fields(keys)) {
			Reflect.setField(dynkeys, i, haxeToDynamo(Reflect.field(keys, i)));
		}
		return buildSpodObject(sendRequest("GetItem", {
			TableName: getTableName(),
			ConsistentRead: consistent,
			Key: dynkeys
		}).Item);
	}
	
	function formatError (httpCode:Int, type:String, message:String):Void {
		var type = type.substr(type.indexOf("#") + 1);
		var message = message;
		
		if (httpCode == 413) throw RequestTooLarge;
		for (i in Type.getEnumConstructs(DynamoDBError)) {
			if (type == i) throw Type.createEnum(DynamoDBError, i);
		}
		for (i in Type.getEnumConstructs(DynamoDBException)) {
			if (type == i) throw Type.createEnum(DynamoDBException, i);
		}
		
		throw "Error: " + type + "\nMessage: " + message;
	}
	
	function sendRequest (operation:String, payload:Dynamic):Dynamic {
		var conn = new aws.auth.Sig4Http((config.ssl ? "https" : "http") + "://" + config.host + "/", config);
		
		conn.setHeader("content-type", "application/x-amz-json-1.0; charset=utf-8");
		conn.setHeader("x-amz-target", SERVICE + "_" + API_VERSION + "." + operation);
		conn.setPostData(haxe.Json.stringify(payload));
		
		var err = null;
		conn.onError = function (msg:String):Void {
			err = msg;
		}
		
		var data = new haxe.io.BytesOutput();
		conn.applySigning(true);
		conn.customRequest(true, data);
		var out:Dynamic;
		try {
			var str = data.getBytes().toString();
			trace(str);
			out = haxe.Json.parse(str);
		} catch (e:Dynamic) {
			throw ConnectionInterrupted;
		}
		if (err != null) formatError(Std.parseInt(err.substr(err.indexOf("#") + 1)), out.__type, out.message);
		return out;
	}
	#end
	
}