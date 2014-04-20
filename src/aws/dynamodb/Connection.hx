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

import aws.auth.IAMConfig;
import aws.auth.Sig4Http;
import aws.dynamodb.DynamoDBError;
import aws.dynamodb.DynamoDBException;
import haxe.crypto.Base64;
import haxe.io.Bytes;
import haxe.io.BytesOutput;
import haxe.Json;

/**
 * Controls all database interaction.
 * @author Sam MacPherson
 */

class Connection {
	
	static inline var SERVICE:String = "DynamoDB";
	static inline var API_VERSION:String = "20120810";
	
	var config:DynamoDBConfig;
	
	/**
	 * Create a new DynamoDB connection.
	 * 
	 * @param	config	An IAM configuration file.
	 */
	public function new (config:DynamoDBConfig) {
		this.config = config;
	}
	
	/**
	 * Creates a table.
	 * 
	 * @param	table	The name of the table you want to create.
	 * @param	key	The primary key definition for this table.
	 * @param	readCapacity	The initial read capacity for this table.
	 * @param	writeCapacity	The initial write capacity for this table.
	 * @return	The details of this table.
	 */
	public function createTable (table:String, ?readCapacity:Int = 1, ?writeCapacity:Int = 1):Void {
		
	}
	
	/**
	 * Deletes a table.
	 * 
	 * @param	table	The name of the table you want to delete.
	 */
	public function deleteTable (table:String):Void {
		
	}
	
	/**
	 * Returns info about the given table.
	 * 
	 * @param	table	The table name.
	 * @return	Information about the table.
	 */
	public function describeTable (table:String):Void {
		
	}
	
	/**
	 * Updates a table's read/write capacity.
	 * 
	 * @param	table	The table's name.
	 * @param	readCapacity	The new read capacity for the table.
	 * @param	writeCapacity	The new write capacity for the table.
	 * @return	Information about the table.
	 */
	public function updateTable (table:String, readCapacity:Int, writeCapacity:Int):Void {
		
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
	
	public function sendRequest (operation:String, payload:Dynamic):Dynamic {
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