package com.amazonaws.dynamodb;

import com.amazonaws.auth.IAMConfig;
import com.amazonaws.auth.Sig4Http;
import com.amazonaws.dynamodb.Collection;
import haxe.BaseCode;
import haxe.io.Bytes;
import haxe.io.BytesOutput;
import haxe.Json;
import sys.net.Socket;

using DateTools;

/**
 * Reaccuring types.
 */

typedef PrimaryKey = {
	hash:Dynamic,
	?range:Dynamic
}

typedef Attribute = Dynamic;

typedef Attributes = Hash<Attribute>;

typedef UpdateAttributes = Hash<{value:Attribute, ?action:String}>;

typedef ComparisonFunction = { values:Array<Dynamic>, op:String };

/**
 * Response types.
 */

typedef ListTablesResponse = {
	tableNames:Array<String>,
	lastEvaluatedTableName:String
}

/**
 * Controls all database interaction.
 * @author Sam MacPherson
 */

class Database {
	
	static inline var BASE64_CHARSET:String = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789/+";
	
	static inline var SERVICE:String = "DynamoDB";
	static inline var API_VERSION:String = "20111205";
	
	static inline var OP_BATCH_GET_ITEM:String = "BatchGetItem";
	static inline var OP_BATCH_WRITE_ITEM:String = "BatchWriteItem";
	static inline var OP_CREATE_TABLE:String = "CreateTable";
	static inline var OP_DELETE_ITEM:String = "DeleteItem";
	static inline var OP_DELETE_TABLE:String = "DeleteTable";
	static inline var OP_DESCRIBE_TABLE:String = "DescribeTable";
	static inline var OP_GET_ITEM:String = "GetItem";
	static inline var OP_LIST_TABLES:String = "ListTables";
	static inline var OP_PUT_ITEM:String = "PutItem";
	static inline var OP_QUERY:String = "Query";
	static inline var OP_SCAN:String = "Scan";
	static inline var OP_UPDATE_ITEM:String = "UpdateItem";
	static inline var OP_UPDATE_TABLE:String = "UpdateTable";
	
	public static inline var UPDATE_PUT:String = "PUT";
	public static inline var UPDATE_ADD:String = "ADD";
	public static inline var UPDATE_DELETE:String = "DELETE";
	
	//Query operators
	public static inline var OP_EQ:String = "EQ";
	public static inline var OP_LE:String = "LE";
	public static inline var OP_LT:String = "LT";
	public static inline var OP_GE:String = "GE";
	public static inline var OP_GT:String = "GT";
	public static inline var OP_BEGINS_WITH:String = "BEGINS_WITH";
	public static inline var OP_BETWEEN:String = "BETWEEN";
	
	//Scan operators
	public static inline var OP_NE:String = "NE";
	public static inline var OP_NOT_NULL:String = "NOT_NULL";
	public static inline var OP_NULL:String = "NULL";
	public static inline var OP_CONTAINS:String = "CONTAINS";
	public static inline var OP_NOT_CONTAINS:String = "NOT_CONTAINS";
	public static inline var OP_IN:String = "IN";
	
	var config:IAMConfig;
	
	/**
	 * Create a new DynamoDB connection.
	 * 
	 * @param	config	An IAM configuration file.
	 */
	public function new (config:IAMConfig) {
		this.config = config;
	}
	
	function mapKeyValue (key:Dynamic):Dynamic {
		if (Std.is(key, String)) {
			return { S:key };
		} else if (Std.is(key, Float) || Std.is(key, Int)) {
			return { N:Std.string(key)};
		} else if (Std.is(key, Bytes)) {
			return { B:BaseCode.encode(key.toString(), BASE64_CHARSET) };
		} else {
			throw "Invalid primary key type. Must be either String, Float, Int or haxe.io.Bytes.";
		}
	}
	
	function mapKey (key:PrimaryKey):Dynamic {
		var obj = { };
		Reflect.setField(obj, "HashKeyElement", mapKeyValue(key.hash));
		if (key.range != null) Reflect.setField(obj, "RangeKeyElement", mapKeyValue(key.range));
		return obj;
	}
	
	function mapKeys (keys:Array<PrimaryKey>):Array<Dynamic> {
		var a = new Array<Dynamic>();
		for (i in keys) {
			a.push(mapKey(i));
		}
		return a;
	}
	
	function mapAttributeValue (data:Dynamic):Dynamic {
		if (Std.is(data, String)) {
			return { S:data };
		} else if (Std.is(data, Float) || Std.is(data, Int)) {
			return { N:Std.string(data) };
		} else if (Std.is(data, Bytes)) {
			return { B:BaseCode.encode(data.toString(), BASE64_CHARSET) };
		} else if (Std.is(data, Array)) {
			var arr:Array<Dynamic> = cast data;
			if (arr.length > 0) {
				var firstElement = arr[0];
				if (Std.is(firstElement, String)) {
					return { SS:data };
				} else if (Std.is(firstElement, Int) || Std.is(firstElement, Float)) {
					return { NS:data };
				} else if (Std.is(firstElement, Bytes)) {
					var a = new Array<String>();
					for (i in cast(data, Array<Dynamic>)) {
						a.push(BaseCode.encode(i.toString(), BASE64_CHARSET));
					}
					return { BS:a };
				} else {
					throw "Invalid attribute set type. Must be either String, Float, Int or haxe.io.Bytes.";
				}
			} else {
				throw "Sets of length 0 are not allowed.";
			}
		} else {
			throw "Invalid attribute type. Must be either String, Float, Int or haxe.io.Bytes or an Array of any scalar type.";
		}
	}
	
	function mapAttributes (data:Attributes):Dynamic {
		var obj = { };
		for (i in data.keys()) {
			var val = data.get(i);
			if (val != null) Reflect.setField(obj, i, mapAttributeValue(val));
		}
		return obj;
	}
	
	function mapAttributeUpdates (data:UpdateAttributes):Dynamic {
		var obj = { };
		for (i in data.keys()) {
			var val = data.get(i);
			var attrib = { Value: mapAttributeValue(val.value) };
			if (val.action != null) Reflect.setField(attrib, "Action", val.action);
			Reflect.setField(obj, i, attrib);
		}
		return obj;
	}
	
	function mapConditional (condition:Attributes):Dynamic {
		var obj = { };
		for (i in condition.keys()) {
			var val = condition.get(i);
			if (val == null) Reflect.setField(obj, i, { Exists: false } );
			else Reflect.setField(obj, i, { Value: mapAttributeValue(val) } );
		}
		return obj;
	}
	
	function mapComparisonFunction (comp:ComparisonFunction):Dynamic {
		var attribValueList = new Array<Dynamic>();
		for (i in comp.values) {
			attribValueList.push(mapAttributeValue(i));
		}
		return { AttributeValueList:attribValueList, ComparisonOperator:comp.op };
	}
	
	function buildKeyValue (data:Dynamic):Dynamic {
		var field = Reflect.fields(data)[0];
		switch (field) {
		case "S": return Reflect.field(data, field);
		case "N":
			var val = Reflect.field(data, field);
			var i = Std.parseInt(val);
			var f = Std.parseFloat(val);
			return i == f ? i : f;
		case "B": return Bytes.ofString(BaseCode.decode(Reflect.field(data, field), BASE64_CHARSET));
		default: throw "Unknown primary key type.";
		}
	}
	
	function buildKey (data:Dynamic):PrimaryKey {
		var hash = buildKeyValue(data.HashKeyElement);
		var range:Dynamic = null;
		if (data.RangeKeyElement != null) return { hash:hash, range:buildKeyValue(data.RangeKeyElement) };
		else return { hash:hash };
	}
	
	function buildKeys (data:Array<Dynamic>):Array<PrimaryKey> {
		var a = new Array<PrimaryKey>();
		for (i in data) {
			a.push(buildKey(i));
		}
		return a;
	}
	
	function buildAttribute (data:Dynamic):Dynamic {
		var field = Reflect.fields(data)[0];
		switch (field) {
		case "S": return Reflect.field(data, field);
		case "N":
			var val = Reflect.field(data, field);
			var i = Std.parseInt(val);
			var f = Std.parseFloat(val);
			return i == f ? i : f;
		case "B": return Bytes.ofString(BaseCode.decode(Reflect.field(data, field), BASE64_CHARSET));
		case "SS": return Reflect.field(data, field);
		case "NS":
			var a = new Array<Dynamic>();
			for (o in cast(Reflect.field(data, field), Array<Dynamic>)) {
				a.push(Std.parseFloat(o));
			}
			return a;
		case "BS":
			var a = new Array<Dynamic>();
			for (i in cast(Reflect.field(data, field), Array<Dynamic>)) {
				a.push(Bytes.ofString(BaseCode.decode(i, BASE64_CHARSET)));
			}
			return a;
		default: throw "Unknown attribute type.";
		}
	}
	
	function buildAttributes (data:Dynamic):Attributes {
		var attribs = new Attributes();
		for (i in Reflect.fields(data)) {
			var field = Reflect.field(data, i);
			if (field != null) attribs.set(i, buildAttribute(field));
		}
		return attribs;
	}
	
	function buildCollectionItems (data:Array<Dynamic>):Array<Attributes> {
		var items = new Array<Attributes>();
		for (i in data) {
			items.push(buildAttributes(i));
		}
		return items;
	}
	
	public function batchGetItems (requestItems:Hash<{keys:Array<PrimaryKey>, ?attributesToGet:Array<String>}>):Hash<Collection> {
		var req = { };
		for (i in requestItems.keys()) {
			var item = requestItems.get(i);
			var obj = { Keys:mapKeys(item.keys) };
			if (item.attributesToGet != null) Reflect.setField(obj, "AttributesToGet", item.attributesToGet);
			Reflect.setField(req, i, obj);
		}
		
		var resp = sendRequest(OP_BATCH_GET_ITEM, req);
		var tables = new Hash<Collection>();
		var unprocessedKeys = new Hash<{keys:Array<PrimaryKey>, attributesToGet:Array<String>}>();
		for (i in Reflect.fields(resp.Responses)) {
			if (i != "UnprocessedKeys") {
				var field = Reflect.field(resp.Responses, i);
				tables.set(i, new BatchedCollection(this, buildCollectionItems(field.Items), field.ConsumedCapacityUnits));
			}
		}
		if (resp.Responses.UnprocessedKeys != null) {
			for (i in Reflect.fields(resp.Responses.UnprocessedKeys)) {
				var field = Reflect.field(resp.Responses.UnprocessedKeys, i);
				var table:BatchedCollection = cast tables.get(i);
				table.setUnprocessedKeys(buildKeys(field.Keys), field.AttributesToGet);
			}
		}
		return tables;
	}
	
	/**
	 * Delete an item from the database.
	 * 
	 * @param	table	The table name you want to delete an item from.
	 * @param	key	The hash key for this item. May be just a hash or a hash and a range key.
	 * @param	?condition	An optional delete condition for atomic evaluation.
	 * @param	?returnOld	If true then this will return the old value of this item.
	 * @return	The old record or null if returnOld is false.
	 */
	public function deleteItem (table:String, key:PrimaryKey, ?condition:Attributes, ?returnOld:Bool = false):Attributes {
		var req = { TableName:table, Key:mapKey(key) };
		if (condition != null) Reflect.setField(req, "Expected", mapConditional(condition));
		if (returnOld) Reflect.setField(req, "ReturnValues", "ALL_OLD");
		
		var resp = sendRequest(OP_DELETE_ITEM, req);
		if (returnOld) return buildAttributes(resp.Attributes);
		else return null;
	}
	
	/**
	 * Lookup an item based on the primary key.
	 * 
	 * @param	table	The table to look in.
	 * @param	key	The primary key for this item
	 * @param	?attributesToGet	A list of attributes to look. If not specified then all attributes are retrieved.
	 * @param	?consistantRead	Set whether the database should use consistant reads. Setting this to true uses 2x as many capacity units.
	 * @return	The attributes for this item.
	 */
	public function getItem (table:String, key:PrimaryKey, ?attributesToGet:Array<String>, ?consistantRead:Bool = false):Attributes {
		var req = { TableName:table, Key:mapKey(key), ConsistentRead:consistantRead }
		if (attributesToGet != null) Reflect.setField(req, "AttributesToGet", attributesToGet);
		
		var resp = sendRequest(OP_GET_ITEM, req);
		return buildAttributes(resp.Item);
	}
	
	/**
	 * List all tables in the database.
	 * 
	 * @param	?limit	An optional upper limit to stop at.
	 * @param	?exclusiveStartTableName	Start at this table name.
	 * @return	A list of table names and potentially a lastEvaluatedTableName if the request didn't finish.
	 */
	public function listTables (?limit:Int, ?exclusiveStartTableName:String):ListTablesResponse {
		var req = { };
		if (limit != null) Reflect.setField(req, "Limit", limit);
		if (exclusiveStartTableName != null) Reflect.setField(req, "ExclusiveStartTableName", exclusiveStartTableName);
		
		var resp = sendRequest(OP_LIST_TABLES, req);
		return { tableNames:resp.TableNames, lastEvaluatedTableName:resp.LastEvaluatedTableName };
	}
	
	/**
	 * Inserts a new item into the database.
	 * 
	 * @param	table	The table name you want to delete an item from.
	 * @param	item	A list of key/value pairs representing the item you want to insert. Must contain a primary key.
	 * @param	?condition	An optional put condition for atomic evaluation.
	 * @param	?returnOld	If true then the old item will be returned.
	 * @return	The old record or null if returnOld is false.
	 */
	public function putItem (table:String, item:Attributes, ?condition:Attributes, ?returnOld:Bool = false):Attributes {
		var req = { TableName:table, Item:mapAttributes(item) };
		if (condition != null) Reflect.setField(req, "Expected", mapConditional(condition));
		if (returnOld) Reflect.setField(req, "ReturnValues", "ALL_OLD");
		
		var resp = sendRequest(OP_PUT_ITEM, req);
		if (returnOld) return buildAttributes(resp.Attributes);
		else return null;
	}
	
	/**
	 * Queries a table for a collection of results from some specific Hash Key. Requires a Hash Key + Range Key table.
	 * 
	 * @param	table	The table name.
	 * @param	hashKey	The hash key portion of the primary key.
	 * @param	?rangeKeyComparisonFunction	A combination of values to compare and the operator to apply. The value list should just be 1 value for everything except the BETWEEN operator.
	 * @param	?attributesToGet	A list of attributes to get. Leave null if you want all attributes or if doing a count.
	 * @param	?limit	Stop after this number of results.
	 * @param	?count	If true then the result will only contain the number of items and not the attributes.
	 * @param	?scanForward	Ascending order or descending.
	 * @param	?consistantRead	Will only return consistant reads. Setting this to true uses 2x as many capacity units per query.
	 * @param	?exclusiveStartKey	Will start the search from the element immediately proceeding this one.
	 * @return	A collection containing the matched items.
	 */
	public function query (table:String, hashKey:Dynamic, ?rangeKeyComparisonFunction:ComparisonFunction, ?attributesToGet:Array<String>, ?limit:Int, ?count:Bool = false, ?scanForward:Bool = true, ?consistantRead:Bool = false, ?exclusiveStartKey:PrimaryKey):Collection {
		var req = { TableName:table, HashKeyValue:mapKeyValue(hashKey), Count:count, ScanIndexForward:scanForward, ConsistentRead:consistantRead };
		if (rangeKeyComparisonFunction != null) Reflect.setField(req, "RangeKeyCondition", mapComparisonFunction(rangeKeyComparisonFunction));
		if (attributesToGet != null) Reflect.setField(req, "AttributesToGet", attributesToGet);
		if (limit != null) Reflect.setField(req, "Limit", limit);
		if (exclusiveStartKey != null) Reflect.setField(req, "ExclusiveStartKey", mapKey(exclusiveStartKey));
		
		var resp = sendRequest(OP_QUERY, req);
		var coll = new QueryScanCollection(this, buildCollectionItems(resp.Items), resp.ConsumedCapacityUnits, table, false, resp.Count, resp.LastEvaluatedKey != null ? buildKey(resp.LastEvaluatedKey) : null);
		coll.hashKey = hashKey;
		coll.comparisonFunction = rangeKeyComparisonFunction;
		coll.attributesToGet = attributesToGet;
		coll._limit = limit;
		coll.countRequest = count;
		coll.scanForward = scanForward;
		coll.consistantRead = consistantRead;
		return coll;
	}
	
	/**
	 * Scans a table for items that match the given filter.
	 * 
	 * @param	table	The table name.
	 * @param	?filters	An attribute-name mapped list of filters you want to apply to the results.
	 * @param	?attributesToGet	A list of attributes to get. Leave null if you want all attributes or if doing a count.
	 * @param	?limit	Stop after this number of results.
	 * @param	?useScannedLimit	Set whether you want to the collection to be considered complete when count == limit (false) or when scannedCount == limit (true).
	 * @param	?count	If true then the result will only contain the number of items and not the attributes.
	 * @param	?exclusiveStartKey	Will start the search from the element immediately proceeding this one.
	 * @return	A collection containing the matched items.
	 */
	public function scan (table:String, ?filters:Hash<ComparisonFunction>, ?attributesToGet:Array<String>, ?limit:Int, ?useScannedLimit:Bool = false, ?count:Bool = false, ?exclusiveStartKey:PrimaryKey):Collection {
		var req = { TableName:table, Count:count };
		if (filters != null) {
			var scanFilters = { };
			for (i in filters.keys()) {
				Reflect.setField(scanFilters, i, mapComparisonFunction(filters.get(i)));
			}
			Reflect.setField(req, "ScanFilter", scanFilters);
		}
		if (attributesToGet != null) Reflect.setField(req, "AttributesToGet", attributesToGet);
		if (limit != null) Reflect.setField(req, "Limit", limit);
		if (exclusiveStartKey != null) Reflect.setField(req, "ExclusiveStartKey", mapKey(exclusiveStartKey));
		
		var resp = sendRequest(OP_SCAN, req);
		var coll = new QueryScanCollection(this, buildCollectionItems(resp.Items), resp.ConsumedCapacityUnits, table, true, resp.Count, resp.LastEvaluatedKey != null ? buildKey(resp.LastEvaluatedKey) : null);
		coll.filters = filters;
		coll.attributesToGet = attributesToGet;
		coll._limit = limit;
		coll.useScannedLimit = useScannedLimit;
		coll.countRequest = count;
		return coll;
	}
	
	/**
	 * Updates an item.
	 * 
	 * @param	table	The table name you want to delete an item from.
	 * @param	key	The hash key for this item. May be just a hash or a hash and a range key.
	 * @param	attributes	A list of attributes to update on the item.
	 * @param	?condition	An optional put condition for atomic evaluation.
	 * @param	?returnNew	true -> Return new records. false -> Return old records. null -> Return nothing.
	 * @param	?returnUpdated	true -> Return only updated records. false -> Return all records.
	 * @return	The record attributes or null if returnNew is null.
	 */
	public function updateItem (table:String, key:PrimaryKey, attributes:UpdateAttributes, ?condition:Attributes, ?returnNew:Bool, ?returnUpdated:Bool = false):Attributes {
		var req = { TableName:table, Key:mapKey(key), AttributeUpdates:mapAttributeUpdates(attributes) };
		if (condition != null) Reflect.setField(req, "Expected", mapConditional(condition));
		if (returnNew == true) {
			if (returnUpdated) Reflect.setField(req, "ReturnValues", "UPDATED_NEW");
			else Reflect.setField(req, "ReturnValues", "ALL_NEW");
		} else if (returnNew == false) {
			if (returnUpdated) Reflect.setField(req, "ReturnValues", "UPDATED_OLD");
			else Reflect.setField(req, "ReturnValues", "ALL_OLD");
		}
		
		var resp = sendRequest(OP_UPDATE_ITEM, req);
		if (returnNew != null) return buildAttributes(resp.Attributes);
		else return null;
	}
	
	function sendRequest (operation:String, payload:Dynamic):Dynamic {
		var conn = new Sig4Http((config.ssl ? "https" : "http") + "://" + config.host + "/", config);
		
		conn.setHeader("content-type", "application/x-amz-json-1.0");
		conn.setHeader("x-amz-target", SERVICE + "_" + API_VERSION + "." + operation);
		conn.setPostData(Json.stringify(payload));
		#if js
		conn.asynch = false;
		#end
		
		var err = null;
		conn.onError = function (msg:String):Void {
			err = msg;
		}
		
		var data:BytesOutput = new BytesOutput();
		conn.applySigning(true);
		conn.customRequest(true, data);
		var out = Json.parse(data.getBytes().toString());
		if (err != null) throw "Http Error: " + err + "\nAWS Error: " + out.__type + "\nMessage: " + out.message;
		return out;
	}
	
}