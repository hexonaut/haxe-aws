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

import chx.hash.HMAC;
import chx.hash.Sha256;
import haxe.Http;
import haxe.io.Bytes;
import haxe.io.BytesOutput;
import haxe.Utf8;

using com.amazonaws.util.ByteTools;
using DateTools;
using StringTools;

/**
 * Extends haxe.Http to sign the request using AWS IAM Sig V2 before sending.
 * Signature format gathered from here: http://docs.aws.amazon.com/general/latest/gr/signature-version-2.html
 * 
 * @author Sam MacPherson
 */

class Sig2Http extends Http {
	
	static inline var sha256 = new Sha256();
	static inline var hmac = new HMAC(sha256);
	
	var _params:Array<{param:String, value:String}>;
	var _headers:Array<{header:String, values:Array<String>}>;
	var _data:String;
	var config:IAMConfig;
	
	/**
	 * Creates a new http connection with Signature V2 authentication.
	 * 
	 * @param	url	The AWS end point.
	 * @param	config	An IAM configuration file.
	 */
	public function new (url:String, config:IAMConfig) {
		super(url);
		
		_params = new Array<{param:String, value:String}>();
		_headers = new Array<{header:String, values:Array<String>}>();
		this.config = config;
		_data = "";
	}
	
	/**
	 * @inheritDoc
	 */
	public override function setParameter (param:String, value:String):Void {
		super.setParameter(param, value);
		
		//Add in the parameter for future signing
		_params.push( { param:param.urlEncode(), value:value.urlEncode() } );
	}
	
	/**
	 * Adds a signature header to the http query. Call this method before submitting the request.
	 * 
	 * @param	post	Set this to be a POST request.
	 */
	public function applySigning (post:Bool):Void {
		//Fill in authorization header before sending
		var buf = new StringBuf();
		
		//List all signed headers
		var signedHeaders = new StringBuf();
		
		//Get the current date in UTC -- need time accurate to the timezone and daylight savings
		var now = Date.now();
		now = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0);
		now = Date.now().delta(now.getTime() - 24. * 3600 * 1000 * Math.floor(now.getTime() / 24 / 3600 / 1000));
		
		//Add in additional query parameters
		setParameter("AWSAccessKeyId", config.accessKey);
		setParameter("SignatureMethod", "HmacSHA256");
		setParameter("SignatureVersion", "2");
		setParameter("Timestamp", now.format("%Y-%m-%dT%H:%M:%S"));
		
		//Apply sort operations on parameters
		_params.sort(function (x, y):Int {
			return x.param > y.param ? 1 : -1;
		});
		
		//Request method
		buf.add(post ? "POST\n" : "GET\n");
		
		//Add the host and uri
		buf.add(config.host + "\n");
		var startIndex = url.indexOf("/", "https://".length);
		var endIndex = url.indexOf("?", startIndex);
		if (endIndex == -1) endIndex = url.length;
		buf.add((startIndex != -1 ? url.substr(startIndex, endIndex - startIndex) : "/") + "\n");
		
		//Query parameters
		for (i in 0 ... _params.length) {
			var entry = _params[i];
			buf.add(entry.param + "=" + entry.value);
			if (i + 1 < _params.length) buf.add("&");
		}
		
		//Add signature
		super.setParameter("Signature", hmac.calculate(Bytes.ofString(config.secretKey), Bytes.ofString(buf.toString())).base64PaddedEncode());
	}
	
}