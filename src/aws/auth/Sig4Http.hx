/****
* Copyright (C) 2013 Sam MacPherson
* 
* Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
* 
* The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
****/

package aws.auth;

import haxe.crypto.Hmac;
import haxe.crypto.Sha256;
import haxe.Http;
import haxe.io.Bytes;
import haxe.io.BytesOutput;
import haxe.Utf8;

using DateTools;
using StringTools;

/**
 * Extends haxe.Http to sign the request using AWS IAM Sig V4 before sending.
 * Signature format gathered from here: http://docs.amazonwebservices.com/general/latest/gr/sigv4-create-canonical-request.html
 * 
 * @author Sam MacPherson
 */

class Sig4Http extends Http {
	
	var _params:Array<{param:String, value:String}>;
	var _headers:Array<{header:String, values:Array<String>}>;
	var _data:String;
	var config:IAMConfig;
	
	/**
	 * Creates a new http connection with Signature V4 authentication.
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
	public override function setParameter (param:String, value:String):Http {
		super.setParameter(param, value);
		
		//Add in the parameter for future signing
		_params.push( { param:param.urlEncode(), value:value.urlEncode() } );
		
		return this;
	}
	
	function headerTrim (value:String):String {
		var buf = new StringBuf();
		
		var lastWasSpace = false;
		var insideQuotes:String = null;
		
		for (i in 0 ... value.length) {
			if (value.charAt(i) == " ") {
				if (!lastWasSpace || insideQuotes != null) {
					buf.add(value.charAt(i));
				}
				
				lastWasSpace = true;
			} else {
				lastWasSpace = false;
				buf.add(value.charAt(i));
				
				if (value.charAt(i) == "\"" || value.charAt(i) == "'") {
					if (insideQuotes == null) insideQuotes = value.charAt(i);
					else if (insideQuotes == value.charAt(i)) insideQuotes = null;
				}
			}
		}
		
		return buf.toString();
	}
	
	function addSigningHeader (header:String, value:String, ?addToRegularHeaders:Bool = true):Void {
		//Add in the header for future signing
		var key = header.toLowerCase();
		var values = value.split(",");
		for (i in 0 ... values.length) {
			values[i] = headerTrim(values[i]);
		}
		for (i in _headers) {
			if (i.header == key) {
				_headers.remove(i);
				break;
			}
		}
		_headers.push( { header:key, values:values } );
		
		if (addToRegularHeaders) setHeader(header, value);
	}
	
	/**
	 * @inheritDoc
	 */
	public override function setPostData (data:String):Http {
		super.setPostData(data);
		
		//setHeader("content-length", Std.string(data.length));
		this._data = data;
		
		return this;
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
		
		//Add in additional query parameters and headers
		addSigningHeader("host", config.host, false);
		if (post) {
			addSigningHeader("x-amz-date", now.format("%Y%m%dT%H%M%SZ"));
		} else {
			//Need signed headers for parameters so do it here
			for (i in 0 ... _headers.length) {
				signedHeaders.add(_headers[i].header);
				if (i + 1 < _headers.length) signedHeaders.add(";");
			}
			setParameter("X-Amz-Algorithm", "AWS4-HMAC-SHA256");
			setParameter("X-Amz-Credential", config.buildCredentialString(now));
			setParameter("X-Amz-Date", now.format("%Y%m%dT%H%M%SZ"));
			setParameter("X-Amz-SignedHeaders", signedHeaders.toString());
		}
		
		//Apply sort operations on parameters and headers
		_params.sort(function (x, y):Int {
			return x.param > y.param ? 1 : -1;
		});
		_headers.sort(function (x, y):Int {
			return x.header > y.header ? 1 : -1;
		});
		
		//Request method
		buf.add(post ? "POST\n" : "GET\n");
		
		//URI
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
		buf.add("\n");
		
		//Request headers
		for (i in 0 ... _headers.length) {
			var entry = _headers[i];
			buf.add(entry.header + ":" + entry.values.join(","));
			buf.add("\n");
		}
		buf.add("\n");
		
		//Add signed headers
		if (post) {
			//If this is a post request this hasn't been calulcated yet
			for (i in 0 ... _headers.length) {
				signedHeaders.add(_headers[i].header);
				if (i + 1 < _headers.length) signedHeaders.add(";");
			}
		}
		buf.add(signedHeaders.toString());
		buf.add("\n");
		
		//Add on payload hash
		buf.add(Sha256.encode(_data));
		
		//Create the string to sign
		var signBuf = new StringBuf();
		signBuf.add("AWS4-HMAC-SHA256\n");
		signBuf.add(now.format("%Y%m%dT%H%M%SZ") + "\n");
		signBuf.add(config.buildCredentialString(now, false) + "\n");
		signBuf.add(Sha256.encode(buf.toString()));
		
		//Derive the signing key
		var hmac = new Hmac(SHA256);
		var derivedKey = hmac.encode(hmac.encode(hmac.encode(hmac.encode(Bytes.ofString("AWS4" + config.secretKey), Bytes.ofString(now.format("%Y%m%d"))), Bytes.ofString(config.region)), Bytes.ofString(config.service)), Bytes.ofString("aws4_request"));
		
		var signature = hmac.encode(derivedKey, Bytes.ofString(signBuf.toString())).toHex();
		
		if (post) {
			super.setHeader("Authorization", "AWS4-HMAC-SHA256 Credential=" + config.buildCredentialString(now) + ", SignedHeaders=" + signedHeaders.toString() + ", Signature=" + signature);
		} else {
			super.setParameter("X-Amz-Signature", signature);
		}
	}
	
}