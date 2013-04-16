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

package chx.hash;

#if neko
import chx.I32;
#end

class Util {
	/**
	**/
	public static function safeAdd(x, y) {
#if !neko
		var lsw = (x & 0xFFFF) + (y & 0xFFFF);
		var msw = (x >> 16) + (y >> 16) + (lsw >> 16);
		return (msw << 16) | (lsw & 0xFFFF);
#else
		if (x == null) x = Int32.make(0, 0);
		if (y == null) y = Int32.make(0, 0);
		var mask = Int32.ofInt(0xFFFF);
		var lsw = Int32.add(Int32.and(x, mask), Int32.and(y, mask));
		var msw = Int32.add(
				Int32.add(Int32.shr(x, 16), Int32.shr(y, 16)),
				Int32.shr(lsw, 16));
		return Int32.or(Int32.shl(msw, 16), Int32.and(lsw, mask));
#end
	}

	/**
		String to big endian binary
		charSize must be 8 or 16 (Unicode)
	**/
	public static function str2binb(str:String, ?charSize:Int) : Array<Int> {
		if(charSize == null)
			charSize = 8;
		if(charSize != 8 && charSize != 16)
			throw "Invalid character size";
		var bin = new Array();
		var mask = (1 << charSize) - 1;
		var i : Int = 0;
		var max : Int = str.length * charSize;
		while(i < max) {
			bin[i>>5] |= (str.charCodeAt(Std.int(i / charSize)) & mask) << (24 - i%32);
			i += charSize;
		}
		return bin;
	}

	public static function binb2hex(binarray:Array<Int>) : String {
  		var hex_tab = Constants.DIGITS_HEXL;
		var sb = new StringBuf();
		for (i in 0...binarray.length * 4) {
			sb.add(
				hex_tab.charAt(
					(binarray[i>>2] >> ((3 - i%4)*8+4)) & 0xF
				)
			);
			sb.add(
				hex_tab.charAt(
					(binarray[i>>2] >> ((3 - i%4)*8  )) & 0xF
				)
			);
  		}
  		return sb.toString();
	}
}



