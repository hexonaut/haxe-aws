package com.amazonaws.auth;

using DateTools;

/**
 * Universal configuration file for all AWS IAM services.
 * @author Sam MacPherson
 */

class IAMConfig {
	
	public var host:String;
	public var accessKey:String;
	public var secretKey:String;
	public var region:String;
	public var service:String;
	public var ssl:Bool;

	/**
	 * Create a new configuration file for some AWS service.
	 * 
	 * @param	host	The AWS end point. This should be of the form "dynamodb.us-west-2.amazonaws.com".
	 * @param	accessKey	Your IAM access key.
	 * @param	secretKey	Your IAM secret access key.
	 * @param	region	The region you want to connect to.
	 * @param	service	The service you want to use. IE "dynamodb".
	 * @param	?ssl	If true then the connection will use https instead of http. May require additional ssl libraries.
	 */
	public function new (host:String, accessKey:String, secretKey:String, region:String, service:String, ?ssl:Bool = true) {
		this.host = host;
		this.accessKey = accessKey;
		this.secretKey = secretKey;
		this.region = region;
		this.service = service;
		this.ssl = ssl;
	}
	
	/**
	 * Mostly used for internal formatting. You can probably ignore this.
	 * 
	 * @param	now	The current datetime.
	 * @param	?includeAccessKey	Include the access key in this credential string.
	 * @return	A formatted credential string as required by AWS IAM authentication.
	 */
	public function buildCredentialString (now:Date, ?includeAccessKey:Bool = true):String {
		var creds = "";
		if (includeAccessKey) creds = accessKey + "/";
		return creds + now.format("%Y%m%d") + "/" + region + "/" + service + "/aws4_request";
	}
	
}