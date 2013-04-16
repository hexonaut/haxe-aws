/****
* Copyright (C) 2013 Sam MacPherson
* 
* Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
* 
* The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
****/

/*
 * Copyright (c) 2008, The Caffeine-hx project contributors
 * Original author : Russell Weir
 * Contributors:
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE CAFFEINE-HX PROJECT CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE CAFFEINE-HX PROJECT CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package chx;

import chx.I32;
import haxe.io.Bytes;
import haxe.io.BytesBuffer;

class BytesUtil {
	/** static 0 length Bytes object **/
	public static var EMPTY : Bytes;

	/////////////////////////////////////////////////////
	//            Public Static methods                //
	/////////////////////////////////////////////////////
	/**
	* Takes an array of byte values, and creates a Bytes buffer. The
	* values in the provided array must all be 0-255
	*
	* @param a Array of byte values
	* @param padToBytes Pad buffer to multiple of, or no padding
	* @throws String if any value in the input array is not in the range 0-255
	**/
	public static function byteArrayToBytes(a: Array<Int>, ?padToBytes:Int) : Bytes  {
		var sb = new BytesBuffer();
		for(i in a) {
			if(i > 0xFF || i < 0)
				throw "Value out of range";
			sb.addByte(i);
		}
		if(padToBytes != null && padToBytes > 0) {
			return nullPad(sb.getBytes(), padToBytes);
		}
		return sb.getBytes();
	}

	/**
		Return a hex representation of the byte b. If
		b > 255 only the lowest 8 bits are used.
	**/
	public static function byteToHex(b : Int) {
		b = b & 0xFF;
		return StringTools.hex(b,2).toLowerCase();
	}

	/**
		Return a hex representation of the byte b. If
		b > 255 only the lowest 8 bits are used.
	**/
	public static function byte32ToHex(b : Int32) {
		var bs : Int = I32.toInt(I32.and(b, I32.ofInt(0xFF)));
		return StringTools.hex(bs,2).toLowerCase();
	}

	/**
		Convert a string containing little endian encoded 32bit integers to an array of int32s<br />
		If the string length is not a multiple of 4, it will be 0 padded
		at the end.
	**/
	public static function bytesToInt32LE(s : Bytes) : Array<Int32>
	{
		return I32.unpackLE(nullPad(s,4));
	}

	/**
	* Cleans out all whitespace and colons from input hex strings, returning
	* a compact, lowercase  version. This will do the following type of conversions:
	* <ul>
	* <li>A0:ff -> a0ff
	* <li>a0 ff -> a0ff
	* <li>a0ff -> a0ff
	* <li>0xFFFF -> ffff
	* </ul>a:ff -> 0aff
	*
	* @param hex Hexadecimal string
	* @return compacted hexadecimal string, with no leading 0x
	**/
	public static function cleanHexFormat(hex : String) : String {
		var e : String = StringTools.replace(hex, ":", "");
		e = e.split("|").join("");
#if (neko || flash9 || js)
		var ereg : EReg = ~/([\s]*)/g;
		e = ereg.replace(e, "");
#else
		e = e.split("\r").join("");
		e = e.split("\n").join("");
		e = e.split("\t").join("");
		e = StringTools.replace(e, " ", "");
		e = StringTools.replace(e, " ", "");
#end
		if(StringTools.startsWith(e, "0x"))
			e = e.substr(2);
		if(e.length & 1 == 1) e = "0" + e;
		return e.toLowerCase();
	}

	/**
	* Encode a buffer to a base specified by the input character set. This
	* is a wrapper to haxe.BaseCode which creates a new BaseCode object
	* on every call, so if speed is required, use BaseCode directly.
	*
	* @param buf Bytes buffer
	* @param base String containing characters for each digit
	* @return new buffer, encoded
	**/
	public static function encodeToBase(buf:Bytes,base:String) : Bytes
	{
		var bc = new haxe.BaseCode( Bytes.ofString(base) );
		return bc.encodeBytes(buf);
	}

	/**
		Tests if two Bytes objects are equal.
	**/
	public static function eq(a:Bytes, b:Bytes) : Bool {
		if (a.length != b.length)
			return false;
		var l = a.length;
		for( i in 0...l)
			if (a.get(i) != b.get(i))
				return false;
		return true;
	}

	/**
	* Dump a buffer to hex bytes. By default, will be seperated with
	* spaces. To have no seperation, use the empty string as a separator.
	*
	* @deprecated use toHex()
	**/
	public static function hexDump(b : Bytes, ?separator:Dynamic) : String {
		return toHex(b, separator);
	}

	/*
		Convert an array of 32bit integers to a little endian Bytes<br />
	**/
	public static inline function int32ToBytesLE(l : Array<Int32>) : Bytes
	{
		return I32.packLE(l);
	}

	/**
		Transform an array of integers x where 0xFF >= x >= 0 to
		a string of binary data, optionally padded to a multiple of
		padToBytes. 0 length input returns 0 length output, not
		padded.
	**/
	public static function int32ArrayToBytes(a: Array<Int32>, ?padToBytes:Int) : Bytes  {
		var sb = new BytesBuffer();
		for(v in a) {
			var i = I32.toInt(v);
			if(i > 0xFF || i < 0)
				throw "Value out of range";
			sb.addByte(i);
		}
		if(padToBytes != null && padToBytes > 0) {
			return nullPad(sb.getBytes(), padToBytes);
		}
		return sb.getBytes();
	}

	/**
		Transform an array of integers x where 0xFF >= x >= 0 to
		a string of binary data, optionally padded to a multiple of
		padToBytes. 0 length input returns 0 length output, not
		padded.
	**/
	public static function intArrayToBytes(a: Array<Int>, ?padToBytes:Int) : Bytes  {
		var sb = new BytesBuffer();
		for(i in a) {
			if(i > 0xFF || i < 0)
				throw "Value out of range";
			sb.addByte(i);
		}
		if(padToBytes != null && padToBytes > 0) {
			return nullPad(sb.getBytes(), padToBytes);
		}
		return sb.getBytes();
	}

	/**
		Create a string initialized to nulls of length len
	**/
	public static function nullBytes( len : Int ) : Bytes {
		var sb = Bytes.alloc(len);
		for(i in 0...len)
			sb.set(i, 0);
		return sb;
	}

	/**
	* Right pad with NULLs to the specified chunk length. Note
	* that 0 length buffer passed to this will not be padded. See also
	* nullBytes()
	*
	* @return Original buffer if no padding required, or new buffer padded.
	**/
	public static function nullPad(s : Bytes, chunkLen: Int) : Bytes {
		var r = chunkLen - (s.length % chunkLen);
		if(r == chunkLen)
			return s;
		var sb = new BytesBuffer();
		sb.add(s);
		for(x in 0...r)
			sb.addByte(0);
		return sb.getBytes();
	}

	/**
	 * Left pad with 'b' to the specified chunk length.
	 *
	 * @param s Bytes to pad
	 * @param chunkLen number of bytes to pad to
	 * @param b Byte to add on left
	 * @return Original buffer if no padding required, or new buffer padded.
	 **/
	public static function leftPad(s : Bytes, chunkLen: Int, b:Null<Int>=0) : Bytes {
		var r = chunkLen - (s.length % chunkLen);
		if(s.length != 0 && r == chunkLen)
			return s;
		var sb = new BytesBuffer();
		for(x in 0...r)
			sb.addByte(b);
		sb.add(s);
		return sb.getBytes();
	}

	public static function ofIntArray(a : Array<Int>) : Bytes {
		var b = new BytesBuffer();
		for(i in 0... a.length) {
			b.addByte(cleanValue(a[i]));
		}
		return b.getBytes();
	}

	/**
	* Parse a hex string into a Bytes. The hex string
	* may start with 0x, may contain spaces, and may contain
	* : delimiters.
	**/
	public static function ofHex(hs : String) : Bytes {
		var s : String = cleanHexFormat(hs);
		var b = new BytesBuffer();
		var l = Std.int(s.length/2);
		for(x in 0...l) {
			var ch = s.substr(x * 2, 2);
			var v = Std.parseInt("0x"+ch);
			if(v > 0xff)
				throw "error";
			b.addByte(v);
		}
		return b.getBytes();
	}

// 	/**
// 		Transform  a string into an array of integers x where
// 		0xFF >= x >= 0, optionally padded to a multiple of
// 		padToBytes. 0 length input returns 0 length output, not
// 		padded.
// 	**/
// 	public static function stringToByteArray( s : String, ?padToBytes:Int) : Array<Int> {
// 		var a = new Array();
// 		var len = s.length;
// 		for(x in 0...s.length) {
// 			a.push(s.charCodeAt(x));
// 		}
// 		if(padToBytes != null && padToBytes > 0) {
// 			var r = padToBytes - (a.length % padToBytes);
// 			if(r != padToBytes) {
// 				for(x in 0...r) {
// 					a.push(0);
// 				}
// 			}
// 		}
// 		return a;
// 	}

	/**
	* Dump a buffer to hex bytes. By default, will be seperated with
	* spaces. To have no seperation, use the empty string as a separator.
	**/
	public static function toHex(b : Bytes, ?separator:Dynamic) : String {
		if(separator == null)
			separator = " ";
		var sb = new StringBuf();
		var l = b.length;
		var first = true;
		for(i in 0...l) {
			if(first) first = false;
			else sb.add(separator);
			sb.add(StringTools.hex(b.get(i),2).toLowerCase());
		}
		return StringTools.rtrim(sb.toString());
	}

	/**
		Remove nulls at the end of a Bytes.
	**/
	public static function unNullPad(s : Bytes) : Bytes {
		var p = s.length - 1;
		while(p-- > 0)
			if(s.get(p) != 0)
				break;
		if(p == 0 && s.get(0) == 0) {
			var bb = new BytesBuffer();
			return bb.getBytes();
		}
		p++;
		var b = Bytes.alloc(p);
		b.blit(0, s, 0, p);
		return b;
	}

	/////////////////////////////////////////////////////
	//                Private methods                  //
	/////////////////////////////////////////////////////
	private static function cleanValue(v : Int) : Int {
		var neg = false;
		if(v < 0) {
			if(v < -128)
				throw "not a byte";
			neg = true;
			v = (v & 0xff) | 0x80;
		}
		if(v > 0xff)
			throw "not a byte";
		return v;
	}

	static function __init__() {
		var bb = new BytesBuffer();
		EMPTY = bb.getBytes();
	}
}
