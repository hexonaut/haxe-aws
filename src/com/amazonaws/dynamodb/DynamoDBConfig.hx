package com.amazonaws.dynamodb;

import com.amazonaws.auth.IAMConfig;

/**
 * DynamoDB specific settings.
 * 
 * @author Sam MacPherson
 */

class DynamoDBConfig extends IAMConfig {
	
	public var throughputRegulator(default, null):Null<ThroughputRegulator>;

	public function new (host:String, accessKey:String, secretKey:String, region:String, service:String) {
		super(host, accessKey, secretKey, region, "dynamodb");
	}
	
	/**
	 * Set the throughput regulator for this database to be this.
	 * 
	 * @param	regulator	The throughput regulator.
	 */
	public function setThroughputRegulator (regulator:ThroughputRegulator):Void {
		this.throughputRegulator = regulator;
	}
	
}