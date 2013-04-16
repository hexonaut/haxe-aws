/****
* Copyright (C) 2013 Sam MacPherson
* 
* Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
* 
* The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
****/



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
	 */
	public function new (host:String, accessKey:String, secretKey:String, region:String, service:String) {
		this.host = host;
		this.accessKey = accessKey;
		this.secretKey = secretKey;
		this.region = region;
		this.service = service;
		this.ssl = true;
	}
	
	/**
	 * Turn on an ssl connection.
	 * 
	 * @param	on	If true then the connection will use https instead of http. May require additional ssl libraries.
	 */
	public function setSSL (on:Bool):Void {
		this.ssl = on;
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