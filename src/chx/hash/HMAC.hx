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

import haxe.io.Bytes;
import haxe.io.BytesBuffer;

/**
	Keyed Hash Message Authentication Codes<br />
	<a href='http://en.wikipedia.org/wiki/Hmac'>Wikipedia entry</a>
**/
class HMAC {
	var hash : IHash;
	var bits : Int;

	/**
	 * Construct a new hmac using the provided hashing method.
	 * @param hashMethod
	 * @param bits If greater than 0, output will be truncated
	 * @todo Bits is only a multiple of 8, but theoretically could implemented with a BigInteger
	 **/
	public function new(hashMethod : IHash, bits : Null<Int>=0) {
		this.hash = hashMethod;
		var hb = hashMethod.getLengthBits();
		if(bits == 0) {
			bits = hb;
		}
		else if(bits > hb){
			bits = hb;
		}
		if(bits <= 0) {
			throw "Invalid HMAC length";
		}
		else if(bits % 8 != 0)
			throw "Bits must be a multiple of 8";
		this.bits = bits;
	}

	public function toString() : String {
		return "hmac-" + (bits>0 ? Std.string(bits)+"-" : "") + Std.string(hash);
	}

	public function dispose() {
		bits = 0;
		hash.dispose();
	}

	public function calculate(key : Bytes, msg : Bytes ) : Bytes {
		var B = hash.getBlockSizeBytes();
		var K : Bytes = key;

		if(K.length > B) {
			K = hash.calculate(K);
		}
		K = BytesUtil.nullPad(K, B);

		var Ki = new BytesBuffer();
		var Ko = new BytesBuffer();
		for (i in 0...K.length) {
			Ko.addByte(K.get(i) ^ 0x5c);
			Ki.addByte(K.get(i) ^ 0x36);
		}
		// hash(Ko + hash(Ki + message))
		Ki.add(msg);
		Ko.add(hash.calculate(Ki.getBytes()));

		// truncated output
		var outer = Ko.getBytes();
		var rv = hash.calculate(outer);
		if(bits > 0 && bits < outer.length * 8)
			rv = rv.sub(0, Std.int(bits/8));
		return rv;
	}

}