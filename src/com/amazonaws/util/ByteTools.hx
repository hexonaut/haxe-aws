/****
* Copyright (C) 2013 Sam MacPherson
* 
* Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
* 
* The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
****/

package com.amazonaws.util;

import haxe.BaseCode;
import haxe.io.Bytes;

/**
 * Contains various utility methods for operating on bytes.
 * 
 * @author Sam MacPherson
 */

class ByteTools {

	static inline var BASE64_CHARSET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	
	public static function base64PaddedEncode (bytes:Bytes):String {
		var size = bytes.length % 3;
		var suffix = "";
		if (size == 1) {
			suffix = "==";
		} else if (size == 2) {
			suffix = "=";
		}
		return BaseCode.encode(bytes.toString(), BASE64_CHARSET) + suffix;
	}
	
	public static function base64PaddedDecode (str:String):Bytes {
		return Bytes.ofString(BaseCode.decode(str.substr(0, str.indexOf("=")), BASE64_CHARSET));
	}
	
}