package com.amazonaws.dynamodb;

import com.amazonaws.auth.IAMConfig;

/**
 * DynamoDB specific settings.
 * 
 * @author Sam MacPherson
 */

class DynamoDBConfig extends IAMConfig {

	public function new (host:String, accessKey:String, secretKey:String, region:String, service:String) {
		super(host, accessKey, secretKey, region, "dynamodb");
	}
	
}