/****
* Copyright (C) 2013 Sam MacPherson
* 
* Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
* 
* The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
****/

package com.amazonaws.dynamodb;

import com.amazonaws.auth.IAMConfig;
import com.amazonaws.auth.Sig4Http;
import com.amazonaws.dynamodb.Collection;
import com.amazonaws.dynamodb.DynamoDBError;
import com.amazonaws.dynamodb.DynamoDBException;
import haxe.BaseCode;
import haxe.io.Bytes;
import haxe.io.BytesOutput;
import haxe.Json;
import sys.net.Socket;

using DateTools;

/**
 * Reaccuring types.
 */

enum DynamoDBType {
	STRING;
	NUMBER;
	BINARY;
}

typedef AttributeDefinition = {
	name:String,
	type:DynamoDBType
}

typedef PrimaryKeyDefinition = {
	hash:AttributeDefinition,
	?range:AttributeDefinition
}

typedef PrimaryKey = {
	hash:Dynamic,
	?range:Dynamic
}

typedef Attribute = Dynamic;

typedef Attributes = Dynamic;

typedef UpdateAttributes = Hash<{value:Attribute, ?action:String}>;

typedef ComparisonFunction = { values:Array<Dynamic>, op:String };

typedef QueryScanResult = { count:Int, consumedCapacityUnits:Int, ?items:Array<Attributes>, ?lastEvaluatedKey:PrimaryKey, ?scannedCount:Int };

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
	
	function base64PaddedEncode (bytes:Bytes):String {
		var size = bytes.length % 3;
		var suffix = "";
		if (size == 1) {
			suffix = "==";
		} else if (size == 2) {
			suffix = "=";
		}
		return BaseCode.encode(bytes.toString(), BASE64_CHARSET) + suffix;
	}
	
	function base64PaddedDecode (str:String):Bytes {
		return Bytes.ofString(BaseCode.decode(str.substr(0, str.indexOf("=")), BASE64_CHARSET));
	}
	
	function mapKeyValue (key:Dynamic):Dynamic {
		if (Std.is(key, String)) {
			return { S:key };
		} else if (Std.is(key, Float) || Std.is(key, Int)) {
			return { N:Std.string(key)};
		} else if (Std.is(key, Bytes)) {
			return { B:base64PaddedEncode(cast(key, Bytes)) };
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
	
	function mapAttributeDefinition (def:AttributeDefinition):Dynamic {
		return { AttributeName:def.name, AttributeType:switch (def.type) {
			case STRING: "S";
			case NUMBER: "N";
			case BINARY: "B";
		} };
	}
	
	function mapKeyDefinition (key:PrimaryKeyDefinition):Dynamic {
		var obj = { };
		Reflect.setField(obj, "HashKeyElement", mapAttributeDefinition(key.hash));
		if (key.range != null) Reflect.setField(obj, "RangeKeyElement", mapAttributeDefinition(key.range));
		return obj;
	}
	
	function mapAttributeValue (data:Dynamic):Dynamic {
		if (Std.is(data, String)) {
			return { S:data };
		} else if (Std.is(data, Float) || Std.is(data, Int)) {
			return { N:Std.string(data) };
		} else if (Std.is(data, Bytes)) {
			return { B:base64PaddedEncode(cast(data, Bytes)) };
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
						a.push(base64PaddedEncode(cast(i, Bytes)));
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
		for (i in Reflect.fields(data)) {
			var val = Reflect.field(data, i);
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
		for (i in Reflect.fields(condition)) {
			var val = Reflect.field(condition, i);
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
		case "B": return base64PaddedDecode(Reflect.field(data, field));
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
		case "B": return base64PaddedDecode(Reflect.field(data, field));
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
				a.push(base64PaddedDecode(i));
			}
			return a;
		default: throw "Unknown attribute type.";
		}
	}
	
	function buildAttributes (data:Dynamic):Attributes {
		var attribs = {};
		for (i in Reflect.fields(data)) {
			var field = Reflect.field(data, i);
			if (field != null) Reflect.setField(attribs, i, buildAttribute(field));
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
	
	/*public function batchGetItems (requestItems:Hash<{keys:Array<PrimaryKey>, ?attributesToGet:Array<String>}>):Hash<Collection> {
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
	}*/
	
	/**
	 * Creates a table.
	 * 
	 * @param	table	The name of the table you want to create.
	 * @param	key	The primary key definition for this table.
	 * @param	readCapacity	The initial read capacity for this table.
	 * @param	writeCapacity	The initial write capacity for this table.
	 * @return	The details of this table.
	 */
	public function createTable (table:String, key:PrimaryKeyDefinition, ?readCapacity:Int = 1, ?writeCapacity:Int = 1):TableInfo {
		var req = { TableName:table, KeySchema:mapKeyDefinition(key), ProvisionedThroughput:{ ReadCapacityUnits:readCapacity, WriteCapacityUnits:writeCapacity } };
		
		return new TableInfo(sendRequest(OP_CREATE_TABLE, req).TableDescription);
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
	 * Deletes a table.
	 * 
	 * @param	table	The name of the table you want to delete.
	 */
	public function deleteTable (table:String):TableInfo {
		var req = { TableName:table };
		
		return new TableInfo(sendRequest(OP_DELETE_TABLE, req).TableDescription);
	}
	
	/**
	 * Returns info about the given table.
	 * 
	 * @param	table	The table name.
	 * @return	Information about the table.
	 */
	public function describeTable (table:String):TableInfo {
		return new TableInfo(sendRequest(OP_DESCRIBE_TABLE, { TableName:table } ).Table);
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
	 * List all tables in the database. May fail and return partial results.
	 * Use getAllTables() for a quick and easy retrieval of all tables.
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
	 * Convenience method to retrieve all the tables in the database.
	 * Use this if the number of tables is small. Otherwise it may be better to use listTables().
	 * 
	 * @return A list of all tables in the database.
	 */
	public function getAllTables ():Array<String> {
		var delay = 1;
		var tables = new Array<String>();
		var lastTableName:String = null;
		while (true) {
			try {
				var resp = listTables(null, lastTableName);
				tables.concat(resp.tableNames);
				lastTableName = resp.lastEvaluatedTableName;
			} catch (e:DynamoDBException) {
				if (delay > 64) throw "Failed to list all tables.";	//Fail after 64 seconds
				
				Sys.sleep(delay);
				delay = delay << 1;
			}
		}
		return tables;
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
	 * @param	?limit	Stop after this number of results. 0 means unlimited.
	 * @param	?count	If true then the result will only contain the number of items and not the attributes.
	 * @param	?scanForward	Ascending order or descending.
	 * @param	?consistantRead	Will only return consistant reads. Setting this to true uses 2x as many capacity units per query.
	 * @param	?exclusiveStartKey	Will start the search from the element immediately proceeding this one.
	 * @return	A list of results as well as some meta data. If count is true then only returns meta data.
	 */
	public function query (table:String, hashKey:Dynamic, ?rangeKeyComparisonFunction:ComparisonFunction, ?attributesToGet:Array<String>, ?limit:Int = 0, ?count:Bool = false, ?scanForward:Bool = true, ?consistantRead:Bool = false, ?exclusiveStartKey:PrimaryKey):QueryScanResult {
		var req = { TableName:table, HashKeyValue:mapKeyValue(hashKey), Count:count, ScanIndexForward:scanForward, ConsistentRead:consistantRead };
		if (rangeKeyComparisonFunction != null) Reflect.setField(req, "RangeKeyCondition", mapComparisonFunction(rangeKeyComparisonFunction));
		if (attributesToGet != null) Reflect.setField(req, "AttributesToGet", attributesToGet);
		if (limit != 0) Reflect.setField(req, "Limit", limit);
		if (exclusiveStartKey != null) Reflect.setField(req, "ExclusiveStartKey", mapKey(exclusiveStartKey));
		
		var resp = sendRequest(OP_QUERY, req);
		var result = { count:resp.Count, consumedCapacityUnits:resp.ConsumedCapacityUnits };
		if (resp.Items != null) Reflect.setField(result, "items", buildCollectionItems(resp.Items));
		if (resp.LastEvaluatedKey != null) Reflect.setField(result, "lastEvaluatedKey", buildKey(resp.LastEvaluatedKey));
		return result;
	}
	
	/**
	 * Scans a table for items that match the given filter.
	 * 
	 * @param	table	The table name.
	 * @param	?filters	An attribute-name mapped list of filters you want to apply to the results.
	 * @param	?attributesToGet	A list of attributes to get. Leave null if you want all attributes or if doing a count.
	 * @param	?scanLimit	Stop after this number of results have been scanned (not necessarily returned). 0 means unlimited.
	 * @param	?count	If true then the result will only contain the number of items and not the attributes.
	 * @param	?exclusiveStartKey	Will start the search from the element immediately proceeding this one.
	 * @return	A list of results as well as some meta data. If count is true then only returns meta data.
	 */
	public function scan (table:String, ?filters:Hash<ComparisonFunction>, ?attributesToGet:Array<String>, ?scanLimit:Int = 0, ?count:Bool = false, ?exclusiveStartKey:PrimaryKey):QueryScanResult {
		var req = { TableName:table, Count:count };
		if (filters != null) {
			var scanFilters = { };
			for (i in filters.keys()) {
				Reflect.setField(scanFilters, i, mapComparisonFunction(filters.get(i)));
			}
			Reflect.setField(req, "ScanFilter", scanFilters);
		}
		if (attributesToGet != null) Reflect.setField(req, "AttributesToGet", attributesToGet);
		if (scanLimit != 0) Reflect.setField(req, "Limit", scanLimit);
		if (exclusiveStartKey != null) Reflect.setField(req, "ExclusiveStartKey", mapKey(exclusiveStartKey));
		
		var resp = sendRequest(OP_SCAN, req);
		var result = { count:resp.Count, consumedCapacityUnits:resp.ConsumedCapacityUnits, scannedCount:resp.ScannedCount };
		if (resp.Items != null) Reflect.setField(result, "items", buildCollectionItems(resp.Items));
		if (resp.LastEvaluatedKey != null) Reflect.setField(result, "lastEvaluatedKey", buildKey(resp.LastEvaluatedKey));
		return result;
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
	
	/**
	 * Updates a table's read/write capacity.
	 * 
	 * @param	table	The table's name.
	 * @param	readCapacity	The new read capacity for the table.
	 * @param	writeCapacity	The new write capacity for the table.
	 * @return	Information about the table.
	 */
	public function updateTable (table:String, readCapacity:Int, writeCapacity:Int):TableInfo {
		return new TableInfo(sendRequest(OP_UPDATE_TABLE, { TableName:table, ProvisionedThroughput:{ ReadCapacityUnits:readCapacity, WriteCapacityUnits:writeCapacity } } ).TableDescription);
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
		var conn = new Sig4Http((config.ssl ? "https" : "http") + "://" + config.host + "/", config);
		
		conn.setHeader("content-type", "application/x-amz-json-1.0; charset=utf-8");
		conn.setHeader("x-amz-target", SERVICE + "_" + API_VERSION + "." + operation);
		conn.setPostData(Json.stringify(payload));
		
		var err = null;
		conn.onError = function (msg:String):Void {
			err = msg;
		}
		
		var data:BytesOutput = new BytesOutput();
		conn.applySigning(true);
		conn.customRequest(true, data);
		var out:Dynamic;
		try {
			out = Json.parse(data.getBytes().toString());
		} catch (e:Dynamic) {
			throw ConnectionInterrupted;
		}
		if (err != null) formatError(Std.parseInt(err.substr(err.indexOf("#") + 1)), out.__type, out.message);
		return out;
	}
	
}