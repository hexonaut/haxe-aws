package aws.dynamodb;

import aws.dynamodb.DynamoDBError;
import aws.dynamodb.DynamoDBException;
import aws.dynamodb.RecordInfos;
import haxe.crypto.Base64;
import haxe.Json;
import haxe.Serializer;
import haxe.Unserializer;

using Lambda;

class Manager<T: #if sys sys.db.Object #else aws.dynamodb.Object #end > {
	
	static inline var SERVICE:String = "DynamoDB";
	static inline var API_VERSION:String = "20120810";
	
	#if !macro
	public static var cnx:Connection;
	public static var prefix:String;	//Global prefix
	#end
	
	var cls:Class<T>;
	var table_infos:RecordInfos;
	var table_keys:Array<String>;

	public function new (cls:Class<T>) {
		this.cls = cls;
		#if !macro
		this.table_infos = getInfos();
		this.table_keys = new Array<String>();
		table_keys.push(table_infos.primaryIndex.hash);
		if (table_infos.primaryIndex.range != null) table_keys.push(table_infos.primaryIndex.range);
		#end
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
	public function getInfos ():RecordInfos {
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
	
	function encodeVal (val:Dynamic, type:RecordType): { t:String, v:Dynamic } {
		return switch (type) {
			case DString: {t:"S", v:val};
			case DFloat, DInt, DDeltaFloat, DDeltaInt: {t:"N", v:Std.string(val)};
			case DBool: {t:"N", v:(val ? "1" : "0")};
			case DDate:
				var date = cast(val, Date);
				date = new Date(date.getFullYear(), date.getMonth(), date.getDate(), 0, 0, 0);
				{t:"N", v:Std.string(date.getTime())};
			case DDateTime: {t:"N", v:Std.string(cast(val, Date).getTime())};
			case DTimeStamp if (Std.is(val, Float)): { t:"N", v:Std.string(val) };
			case DTimeStamp: { t:"N", v:Std.string(cast(val, Date).getTime()) };
			case DBinary: {t:"B", v:Base64.encode(val)};
			case DEnum(e) if (Std.is(val, Int)): { t:"N", v:Std.string(val) };
			case DEnum(e): { t:"N", v:Std.string(Type.enumIndex(val)) };
			case DStringEnum(e) if (Std.is(val, Int)): { t:"S", v:Std.string(Type.createEnumIndex(e, val)) };
			case DStringEnum(e): { t:"S", v:Std.string(val) };
			case DData: { t:"S", v:Serializer.run(val) };
			case DSet(t), DUniqueSet(t):
				var dtype = switch (t) {
					case DString: "SS";
					case DBinary: "BS";
					default: "NS";
				}
				var list = new Array<Dynamic>();
				for (i in cast(val, Array<Dynamic>)) {
					list.push(encodeVal(i, t).v);
				}
				if (list.length == 0) throw "Set must contain at least one value.";
				{t:dtype, v:list};
		};
	}
	
	public function haxeToDynamo (name:String, v:Dynamic):Dynamic {
		if (v == null) return null;
		
		var obj:Dynamic = { };
		var ev = encodeVal(v, getFieldType(name));
		
		Reflect.setField(obj, ev.t, ev.v);
		
		return obj;
	}
	
	function decodeVal (val:Dynamic, type:RecordType):Dynamic {
		return switch (type) {
			case DString: val;
			case DFloat, DDeltaFloat: Std.parseFloat(val);
			case DInt, DDeltaInt: Std.parseInt(val);
			case DBool: val == "1";
			case DDate, DDateTime: Date.fromTime(Std.parseFloat(val));
			case DTimeStamp: Std.parseFloat(val);
			case DBinary: Base64.decode(val);
			case DEnum(e): Type.createEnumIndex(e, val);
			case DStringEnum(e): Type.createEnum(e, val);
			case DData: Unserializer.run(val);
			case DSet(t), DUniqueSet(t):
				var list = new Array<Dynamic>();
				for (i in cast(val, Array<Dynamic>)) {
					list.push(decodeVal(i, t));
				}
				list;
		};
	}
	
	public function dynamoToHaxe (name:String, v:Dynamic):Dynamic {
		if (v == null) return null;
		
		var infos = getInfos();
		
		for (i in Reflect.fields(v)) {
			var val = Reflect.field(v, i);
			if (val == null) return null;
			return decodeVal(val, getFieldType(name));
		}
		
		throw "Unknown DynamoDB type.";
	}
	
	public function duplicate (type:RecordType, v:Dynamic):Dynamic {
		//TODO this could probably be optimized
		return decodeVal(encodeVal(v, type), type);
	}
	
	function buildSpodObject (item:Dynamic):Null<T> {
		if (Reflect.fields(item).length == 0) return null;
		
		var infos = getInfos();
		
		var spod = Type.createInstance(cls, []);
		var lastSeen:Dynamic = {};
		for (i in Reflect.fields(item)) {
			if (infos.fields.exists(function (e) { return e.name == i; } )) {
				Reflect.setField(spod, i, dynamoToHaxe(i, Reflect.field(item, i)));
				Reflect.setField(lastSeen, i, dynamoToHaxe(i, Reflect.field(item, i)));
			}
		}
		//__last used to check for fields that have changed since last sync
		Reflect.setField(spod, "__last", lastSeen);
		return spod;
	}
	
	function getTableName (?shardDate:Date):String {
		var infos = getInfos();
		if (shardDate == null) shardDate = Date.now();
		
		var str = "";
		if (infos.prefix != null) {
			str += infos.prefix;
		} else if (prefix != null) {
			str += prefix;
		}
		str += infos.table;
		if (infos.shard != null) {
			str += DateTools.format(shardDate, infos.shard);
		}
		return str;
	}
	
	public function unsafeGet (id:Dynamic, ?consistent:Bool = false): #if js promhx.Promise<T> #else T #end {
		var infos = getInfos();
		var keys:Dynamic = { };
		Reflect.setField(keys, infos.primaryIndex.hash, id);
		return unsafeGetWithKeys(keys, consistent);
	}
	
	public function unsafeGetWithKeys (keys:Dynamic, ?consistent:Bool = false): #if js promhx.Promise<T> #else T #end {
		var dynkeys:Dynamic = { };
		for (i in Reflect.fields(keys)) {
			Reflect.setField(dynkeys, i, haxeToDynamo(i, Reflect.field(keys, i)));
		}
		#if js
		return cnx.sendRequest("GetItem", {
			TableName: getTableName(),
			ConsistentRead: consistent,
			Key: dynkeys
		}).then(function (result:Dynamic) {
			return buildSpodObject(result.Item);
		});
		#else
		return buildSpodObject(cnx.sendRequest("GetItem", {
			TableName: getTableName(),
			ConsistentRead: consistent,
			Key: dynkeys
		}).Item);
		#end
	}
	
	public function unsafeObjects (query:Dynamic, ?consistent:Bool = false): #if js promhx.Promise<List<T>> #else List<T> #end {
		//Check if start key is null
		var startKey = Reflect.field(query, "ExclusiveStartKey");
		if (startKey != null) {
			if (Reflect.field(startKey, Reflect.fields(startKey)[0]) == null) {
				Reflect.deleteField(query, "ExclusiveStartKey");
			}
		}
		
		Reflect.setField(query, "TableName", getTableName());
		Reflect.setField(query, "ConsistentRead", consistent);
		#if js
		return cnx.sendRequest("Query", query).then(function (result) {
			return Lambda.map(cast(result.Items, Array<Dynamic>), function (e) { return buildSpodObject(e); } );
		});
		#else
		return Lambda.map(cast(cnx.sendRequest("Query", query).Items, Array<Dynamic>), function (e) { return buildSpodObject(e); } );
		#end
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
	
	function hasChanged (type:RecordType, val1:Dynamic, val2:Dynamic):Bool {
		return switch (type) {
			case DString, DFloat, DBool, DInt, DDeltaInt, DDeltaFloat: val1 != val2;
			case DDate, DDateTime: (val1 == null && val2 != null) || (val1 != null && val2 == null) || val1.getTime() != val2.getTime();
			case DTimeStamp:
				if (val1 != null && Std.is(val1, Float)) val1 = Date.fromTime(val1);
				if (val2 != null && Std.is(val2, Float)) val2 = Date.fromTime(val2);
				(val1 == null && val2 != null) || (val1 != null && val2 == null) || (val1 != null && val2 != null && val1.getTime() != val2.getTime());
			case DBinary: (val1 == null && val2 != null) || (val1 != null && val2 == null) || val1.toHex() != val2.toHex();
			case DEnum(e):
				if (val1 != null && !Std.is(val1, Int)) val1 = Type.enumIndex(val1);
				if (val2 != null && !Std.is(val2, Int)) val2 = Type.enumIndex(val2);
				val1 != val2;
			case DStringEnum(e):
				if (val1 != null && !Std.is(val1, Int)) val1 = Std.string(val1);
				if (val2 != null && !Std.is(val2, Int)) val2 = Std.string(val2);
				val1 != val2;
			case DData:
				Serializer.run(val1) != Serializer.run(val2);
			case DSet(t), DUniqueSet(t):
				//Make sure list lengths match
				if (val1 != null && val2 != null && val1.length == val2.length) {
					var diff = false;
					var arr1:Array<Dynamic> = Lambda.array(val1);
					var arr2:Array<Dynamic> = Lambda.array(val2);
					for (i in 0 ... arr1.length) {
						if (hasChanged(t, arr1[i], arr2[i])) {
							diff = true;
							
							break;
						}
					}
					diff;
				} else {
					(val1 == null && val2 != null) || (val1 != null && val2 == null) || (val1 != null && val2 != null && val1.length != val2.length);
				}
		};
	}
	
	function arrDiff (oldArr:Array<Dynamic>, newArr:Array<Dynamic>): { add:Array<Dynamic>, remove:Array<Dynamic> } {
		var add = new Array<Dynamic>();
		var remove = new Array<Dynamic>();
		
		for (i in oldArr) {
			if (!Lambda.has(newArr, i)) {
				remove.push(i);
			}
		}
		for (i in newArr) {
			if (!Lambda.has(oldArr, i)) {
				add.push(i);
			}
		}
		
		return { add:add, remove:remove };
	}
	
	function buildUpdateFields (spod:T):Dynamic {
		var infos = getInfos();
		var fields:Dynamic = { };
		
		for (i in infos.fields) {
			var v:Dynamic = Reflect.field(spod, i.name);
			var oldVal:Dynamic = Reflect.field(Reflect.field(spod, "__last"), i.name);
			if (hasChanged(i.type, v, oldVal)) {
				if (v != null) {
					if (Std.is(v, String) && cast(v, String).length == 0) throw "String values must have length greater than 0.";
					
					if (i.type.match(DDeltaFloat | DDeltaInt | DUniqueSet(_)) && oldVal != null) {
						if (i.type.match(DUniqueSet(_))) {
							var diff = arrDiff(oldVal, v);
							if (diff.add.length > 0) Reflect.setField(fields, i.name, { Action:"ADD", Value:haxeToDynamo(i.name, diff.add) } );
							if (diff.remove.length > 0) Reflect.setField(fields, i.name, { Action:"DELETE", Value:haxeToDynamo(i.name, diff.remove) } );
						} else {
							Reflect.setField(fields, i.name, { Action:"ADD", Value:haxeToDynamo(i.name, v - oldVal) } );
						}
					} else {
						Reflect.setField(fields, i.name, { Action:"PUT", Value:haxeToDynamo(i.name, v) } );
					}
				} else {
					Reflect.setField(fields, i.name, { Action:"DELETE" });
				}
			}
		}
		
		return fields;
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
	
	public function doInsert (obj:T): #if js promhx.Promise<T> #else Void #end {
		var infos = getInfos();
		checkKeyExists(obj, infos.primaryIndex);
		var item = buildFields(obj);
		
		var result = cnx.sendRequest("PutItem ", {
			TableName: getTableName(),
			Expected: buildRecordExpected(obj, infos.primaryIndex, false),
			Item: item
		});
		#if js
		return result.then(function (_) {
			var last:Dynamic = {};
			for (i in Reflect.fields(item)) {
				Reflect.setField(last, i, dynamoToHaxe(i, Reflect.field(item, i)));
			}
			Reflect.setField(obj, "__last", last);
			return obj;
		});
		#else
		var last:Dynamic = {};
		for (i in Reflect.fields(item)) {
			Reflect.setField(last, i, dynamoToHaxe(i, Reflect.field(item, i)));
		}
		Reflect.setField(obj, "__last", last);
		#end
	}
	
	public function doUpdate (obj:T): #if js promhx.Promise<T> #else Void #end {
		return doConditionalUpdate (obj, null);
	}
	
	function convertUpdateFieldsToExpr (fields:Dynamic, attribValues:Dynamic, attribNames:Dynamic):String {
		var index = 0;
		var sets = new Array<String>();
		var adds = new Array<String>();
		var deletes = new Array<String>();
		var removes = new Array<String>();
		
		for (i in Reflect.fields(fields)) {
			var item = Reflect.field(fields, i);
			var av = ':dv${index++}';
			var an = '#dn${index++}';
			Reflect.setField(attribNames, an, i);
			switch (item.Action) {
				case "PUT":
					Reflect.setField(attribValues, av, item.Value);
					sets.push('$an = $av');
				case "ADD":
					Reflect.setField(attribValues, av, item.Value);
					adds.push('$an $av');
				case "DELETE":
					if (item.Value != null) {
						Reflect.setField(attribValues, av, item.Value);
						deletes.push('$an $av');
					} else {
						removes.push('$an');
					}
				default:
			}
		}
		
		var str = "";
		if (sets.length > 0) str += 'SET ${sets.join(", ")} ';
		if (adds.length > 0) str += 'ADD ${adds.join(", ")} ';
		if (deletes.length > 0) str += 'DELETE ${deletes.join(", ")} ';
		if (removes.length > 0) str += 'REMOVE ${removes.join(", ")} ';
		return str;
	}
	
	public function doConditionalUpdate (obj:T, ?condition: { attribNames:Dynamic, attribValues:Dynamic, expr:Dynamic }): #if js promhx.Promise<T> #else Void #end {
		var infos = getInfos();
		checkKeyExists(obj, infos.primaryIndex);
		
		var key = { };
		Reflect.setField(key, infos.primaryIndex.hash, haxeToDynamo(infos.primaryIndex.hash, Reflect.field(obj, infos.primaryIndex.hash)));
		if (infos.primaryIndex.range != null) Reflect.setField(key, infos.primaryIndex.range, haxeToDynamo(infos.primaryIndex.range, Reflect.field(obj, infos.primaryIndex.range)));
		
		var updateFields = buildUpdateFields(obj);
		if (Reflect.fields(updateFields).length == 0) {
			#if js
			return promhx.Promise.promise(obj);
			#else
			return;
			#end
		}
		var query = {
			TableName: getTableName(),
			Key: key
		};
		if (condition != null) {
			//Add prefixes to names and values
			for (i in Reflect.fields(condition.attribNames)) {
				Reflect.setField(condition.attribNames, '#$i', Reflect.field(condition.attribNames, i));
				Reflect.deleteField(condition.attribNames, i);
			}
			for (i in Reflect.fields(condition.attribValues)) {
				Reflect.setField(condition.attribValues, ':$i', Reflect.field(condition.attribValues, i));
				Reflect.deleteField(condition.attribValues, i);
			}
			
			Reflect.setField(query, "ConditionExpression", condition.expr);
			Reflect.setField(query, "ExpressionAttributeNames", condition.attribNames);
			Reflect.setField(query, "ExpressionAttributeValues", condition.attribValues);
			Reflect.setField(query, "UpdateExpression", convertUpdateFieldsToExpr(updateFields, condition.attribValues, condition.attribNames));
		} else {
			Reflect.setField(query, "AttributeUpdates", updateFields);
		}
		var objLast = Reflect.field(obj, "__last");
		if (objLast == null) return promhx.Promise.error("Object not in database.");
		var result = cnx.sendRequest("UpdateItem ", query);
		
		#if js
		//Update last fields
		var oldLast:Dynamic = { };
		for (i in Reflect.fields(updateFields)) {
			Reflect.setField(oldLast, i, dynamoToHaxe(i, haxeToDynamo(i, Reflect.field(Reflect.field(obj, "__last"), i))));
		}
		for (i in Reflect.fields(updateFields)) {
			Reflect.setField(objLast, i, dynamoToHaxe(i, haxeToDynamo(i, Reflect.field(obj, i))));
		}
		
		return result.then(function (_) {
			return obj;
		}).errorPipe(function (err:Dynamic) {
			//Reset update fields
			for (i in Reflect.fields(updateFields)) {
				Reflect.setField(Reflect.field(obj, "__last"), i, dynamoToHaxe(i, haxeToDynamo(i, Reflect.field(oldLast, i))));
			}
			
			return promhx.Promise.error(err);
		});
		#else
		for (i in Reflect.fields(updateFields)) {
			Reflect.setField(Reflect.field(obj, "__last"), i, dynamoToHaxe(i, haxeToDynamo(i, Reflect.field(obj, i))));
		}
		#end
	}
	
	public function doPut (obj:T): #if js promhx.Promise<T> #else Void #end {
		var infos = getInfos();
		checkKeyExists(obj, infos.primaryIndex);
		
		var result = cnx.sendRequest("PutItem ", {
			TableName: getTableName(),
			Item: buildFields(obj)
		});
		#if js
		return result.then(function (_) {
			return obj;
		});
		#end
	}
	
	public function doDelete (obj:T): #if js promhx.Promise<T> #else Void #end {
		var infos = getInfos();
		checkKeyExists(obj, infos.primaryIndex);
		
		var key = { };
		Reflect.setField(key, infos.primaryIndex.hash, haxeToDynamo(infos.primaryIndex.hash, Reflect.field(obj, infos.primaryIndex.hash)));
		if (infos.primaryIndex.range != null) Reflect.setField(key, infos.primaryIndex.range, haxeToDynamo(infos.primaryIndex.range, Reflect.field(obj, infos.primaryIndex.range)));
		
		var result = cnx.sendRequest("DeleteItem ", {
			TableName: getTableName(),
			Key: key
		});
		#if js
		return result.then(function (_) {
			return obj;
		});
		#end
	}
	
	public function doSerialize( field : String, v : Dynamic ) : haxe.io.Bytes {
		var s = new haxe.Serializer();
		s.useEnumIndex = true;
		s.serialize(v);
		var str = s.toString();
		#if neko
		return neko.Lib.bytesReference(str);
		#else
		return haxe.io.Bytes.ofString(str);
		#end
	}

	public function doUnserialize( field : String, b : haxe.io.Bytes ) : Dynamic {
		if( b == null )
			return null;
		var str;
		#if neko
		str = neko.Lib.stringReference(b);
		#else
		str = b.toString();
		#end
		if( str == "" )
			return null;
		return haxe.Unserializer.run(str);
	}
	
	function objectToString( it : T ) : String {
		var table_name = getTableName();
		var s = new StringBuf();
		s.add(table_name);
		if( table_keys.length == 1 ) {
			s.add("#");
			s.add(Reflect.field(it,table_keys[0]));
		} else {
			s.add("(");
			var first = true;
			for( f in table_keys ) {
				if( first )
					first = false;
				else
					s.add(",");
				s.add(f);
				s.add(":");
				s.add(Reflect.field(it,f));
			}
			s.add(")");
		}
		return s.toString();
	}
	
	function makeCacheKey( x : T ) : String {
		var table_name = getTableName();
		if( table_keys.length == 1 ) {
			var k = Reflect.field(x,table_keys[0]);
			if( k == null )
				throw("Missing key "+table_keys[0]);
			return Std.string(k)+table_name;
		}
		var s = new StringBuf();
		for( k in table_keys ) {
			var v = Reflect.field(x,k);
			if( k == null )
				throw("Missing key "+k);
			s.add(v);
			s.add("#");
		}
		s.add(table_name);
		return s.toString();
	}
	
	public function createTable (?shardDate:Date): #if js promhx.Promise<Dynamic> #else Void #end {
		var infos = getInfos();
		
		var attrFields = new Array<String>();
		
		var key = new Array<Dynamic>();
		key.push( { AttributeName:infos.primaryIndex.hash, KeyType:"HASH" } );
		attrFields.push(infos.primaryIndex.hash);
		if (infos.primaryIndex.range != null) {
			key.push( { AttributeName:infos.primaryIndex.range, KeyType:"RANGE" } );
			attrFields.push(infos.primaryIndex.range);
		}
		
		var globalIndexes = new Array<Dynamic>();
		var localIndexes = new Array<Dynamic>();
		for (i in infos.indexes) {
			var key = new Array<Dynamic>();
			key.push( { AttributeName:i.index.hash, KeyType:"HASH" } );
			if (i.index.range != null) key.push( { AttributeName:infos.primaryIndex.range, KeyType:"RANGE" } );
			
			if (i.global) {
				globalIndexes.push( {
					IndexName: i.name,
					KeySchema: key,
					Projection: { ProjectionType: "ALL" },
					ProvisionedThroughput: {
						ReadCapacityUnits: i.readCap != null ? i.readCap : 1,
						WriteCapacityUnits: i.writeCap != null ? i.writeCap : 1
					}
				} );
				
				if (!attrFields.has(i.index.hash)) attrFields.push(i.index.hash);
				if (i.index.range != null) {
					if (!attrFields.has(i.index.range)) attrFields.push(i.index.range);
				}
			} else {
				localIndexes.push( {
					IndexName: i.name,
					KeySchema: key,
					Projection: { ProjectionType: "ALL" }
				} );
			}
		}
		
		var fields = new Array<Dynamic>();
		for (i in infos.fields) {
			if (attrFields.has(i.name)) {
				var type = switch (i.type) {
					case DString: "S";
					case DBinary: "B";
					case DSet(t):
						switch (t) {
							case DString: "SS";
							case DBinary: "BS";
							default: "NS";
						}
					default: "N";
				};
				
				fields.push( {
					AttributeName: i.name,
					AttributeType: type
				} );
			}
		}
		
		var req = {
			TableName: getTableName(shardDate),
			ProvisionedThroughput: {
				ReadCapacityUnits: infos.readCap != null ? infos.readCap : 1,
				WriteCapacityUnits: infos.writeCap != null ? infos.writeCap : 1
			},
			KeySchema: key,
			AttributeDefinitions: fields
		};
		
		if (globalIndexes.length > 0) Reflect.setField(req, "GlobalSecondaryIndexes", globalIndexes);
		if (localIndexes.length > 0) Reflect.setField(req, "LocalSecondaryIndexes", localIndexes);
		
		var result = cnx.sendRequest("CreateTable", req);
		#if js
		return result.then(function (_) {
			return null;
		});
		#end
	}
	
	public function deleteTable (?shardDate:Date): #if js promhx.Promise<Dynamic> #else Void #end {
		var result = cnx.sendRequest("DeleteTable", { TableName:getTableName(shardDate) } );
		#if js
		return result.then(function (_) {
			return null;
		});
		#end
	}
	
	public function tableExists (?shardDate:Date): #if js promhx.Promise<Bool> #else Bool #end {
		#if js
		return cast cnx.sendRequest("DescribeTable", { TableName:getTableName(shardDate) } ).then(function (_) {
			return true;
		}).errorThen(function (_) {
			return false;
		});
		#else
		try {
			cnx.sendRequest("DescribeTable", { TableName:getTableName(shardDate) } );
			return true;
		} catch (e:DynamoDBError) {
			if (e == ResourceNotFoundException) {
				return false;
			} else {
				#if neko
				neko.Lib.rethrow(e);
				#elseif cpp
				cpp.Lib.rethrow(e);
				#else
				throw e;
				#end
				return null;
			}
		}
		#end
	}
	#end
	
}