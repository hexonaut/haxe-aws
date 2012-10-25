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

/*
 * Adapted from:
 * A JavaScript implementation of the Secure Hash Algorithm, SHA-256
 * Version 0.3 Copyright Angel Marin 2003-2004 - http://anmar.eu.org/
 * http://anmar.eu.org/projects/jssha2/
 * Distributed under the BSD License
 * Some bits taken from Paul Johnston's SHA-1 implementation
 */

package chx.hash;

import chx.BytesUtil;
import chx.I32;
import haxe.io.Bytes;

class Sha256 implements IHash {
	/**
	 * Length of Sha256 hashes
	 **/
	public static inline var BYTES : Int = 32;

	public function new() {
	}

	public function toString() : String {
		return "sha256";
	}

	public function calculate( msg:Bytes ) : Bytes {
		return encode(msg);
	}

	public function calcHex( msg:Bytes ) : String {
		return encode(msg).toHex();
	}

	public function getLengthBytes() : Int {
		return 32;
	}

	public function getLengthBits() : Int {
		return 256;
	}

	public function getBlockSizeBytes() : Int {
		return 64;
	}

	public function getBlockSizeBits() : Int {
		return 512;
	}

	public function dispose() : Void {
		#if !(neko || useOpenSSL)
		#end
	}

//#if !(neko || useOpenSSL)
	private static var charSize : Int = 8;
	public static function encode(s : Bytes) : Bytes {
		var pb : Array<Int32> = I32.unpackBE(BytesUtil.nullPad(s,4));
		var res = core_sha256(pb, s.length * charSize);
		return I32.packBE(cast res);
	}

	static inline function S (X, n) {
		#if !neko
		return ( X >>> n ) | (X << (32 - n));
		#else
		if (X == null) X = Int32.make(0, 0);
		return Int32.or(Int32.ushr(X, n), Int32.shl(X, (32 - n)));
		#end
	}
	static inline function R (X, n) {
		#if !neko
		return ( X >>> n );
		#else
		if (X == null) X = Int32.make(0, 0);
		return Int32.ushr(X, n);
		#end
	}
	static inline function Ch(x, y, z) {
		#if !neko
		return ((x & y) ^ ((~x) & z));
		#else
		if (x == null) x = Int32.make(0, 0);
		if (y == null) y = Int32.make(0, 0);
		if (z == null) z = Int32.make(0, 0);
		return Int32.xor(Int32.and(x, y), Int32.and(Int32.complement(x), z));
		#end
	}
	static inline function Maj(x, y, z) {
		#if !neko
		return ((x & y) ^ (x & z) ^ (y & z));
		#else
		if (x == null) x = Int32.make(0, 0);
		if (y == null) y = Int32.make(0, 0);
		if (z == null) z = Int32.make(0, 0);
		return Int32.xor(Int32.xor(Int32.and(x, y), Int32.and(x, z)), Int32.and(y, z));
		#end
	}
	static inline function Sigma0256(x) {
		#if !neko
		return (S(x, 2) ^ S(x, 13) ^ S(x, 22));
		#else
		return Int32.xor(Int32.xor(S(x, 2), S(x, 13)), S(x, 22));
		#end
	}
	static inline function Sigma1256(x) {
		#if !neko
		return (S(x, 6) ^ S(x, 11) ^ S(x, 25));
		#else
		return Int32.xor(Int32.xor(S(x, 6), S(x, 11)), S(x, 25));
		#end
	}
	static inline function Gamma0256(x) {
		#if !neko
		return (S(x, 7) ^ S(x, 18) ^ R(x, 3));
		#else
		return Int32.xor(Int32.xor(S(x, 7), S(x, 18)), R(x, 3));
		#end
	}
	static inline function Gamma1256(x) {
		#if !neko
		return (S(x, 17) ^ S(x, 19) ^ R(x, 10));
		#else
		return Int32.xor(Int32.xor(S(x, 17), S(x, 19)), R(x, 10));
		#end
	}
	static function core_sha256 (m:Array<Int32>, l) {
		#if !neko
		var K : Array<Int> = [
			0x428A2F98,0x71374491,0xB5C0FBCF,0xE9B5DBA5,0x3956C25B,
			0x59F111F1,0x923F82A4,0xAB1C5ED5,0xD807AA98,0x12835B01,
			0x243185BE,0x550C7DC3,0x72BE5D74,0x80DEB1FE,0x9BDC06A7,
			0xC19BF174,0xE49B69C1,0xEFBE4786,0xFC19DC6,0x240CA1CC,
			0x2DE92C6F,0x4A7484AA,0x5CB0A9DC,0x76F988DA,0x983E5152,
			0xA831C66D,0xB00327C8,0xBF597FC7,0xC6E00BF3,0xD5A79147,
			0x6CA6351,0x14292967,0x27B70A85,0x2E1B2138,0x4D2C6DFC,
			0x53380D13,0x650A7354,0x766A0ABB,0x81C2C92E,0x92722C85,
			0xA2BFE8A1,0xA81A664B,0xC24B8B70,0xC76C51A3,0xD192E819,
			0xD6990624,0xF40E3585,0x106AA070,0x19A4C116,0x1E376C08,
			0x2748774C,0x34B0BCB5,0x391C0CB3,0x4ED8AA4A,0x5B9CCA4F,
			0x682E6FF3,0x748F82EE,0x78A5636F,0x84C87814,0x8CC70208,
			0x90BEFFFA,0xA4506CEB,0xBEF9A3F7,0xC67178F2
		];
		var HASH : Array<Int> = [
			0x6A09E667,0xBB67AE85,0x3C6EF372,0xA54FF53A,
			0x510E527F,0x9B05688C,0x1F83D9AB,0x5BE0CD19
		];
		#else
		var K : Array<Int32> = [
			Int32.make(0x428A, 0x2F98),Int32.make(0x7137, 0x4491),Int32.make(0xB5C0, 0xFBCF),Int32.make(0xE9B5, 0xDBA5),Int32.make(0x3956, 0xC25B),
			Int32.make(0x59F1, 0x11F1),Int32.make(0x923F, 0x82A4),Int32.make(0xAB1C, 0x5ED5),Int32.make(0xD807, 0xAA98),Int32.make(0x1283, 0x5B01),
			Int32.make(0x2431, 0x85BE),Int32.make(0x550C, 0x7DC3),Int32.make(0x72BE, 0x5D74),Int32.make(0x80DE, 0xB1FE),Int32.make(0x9BDC, 0x06A7),
			Int32.make(0xC19B, 0xF174),Int32.make(0xE49B, 0x69C1),Int32.make(0xEFBE, 0x4786),Int32.make(0x0FC1, 0x9DC6),Int32.make(0x240C, 0xA1CC),
			Int32.make(0x2DE9, 0x2C6F),Int32.make(0x4A74, 0x84AA),Int32.make(0x5CB0, 0xA9DC),Int32.make(0x76F9, 0x88DA),Int32.make(0x983E, 0x5152),
			Int32.make(0xA831, 0xC66D),Int32.make(0xB003, 0x27C8),Int32.make(0xBF59, 0x7FC7),Int32.make(0xC6E0, 0x0BF3),Int32.make(0xD5A7, 0x9147),
			Int32.make(0x06CA, 0x6351),Int32.make(0x1429, 0x2967),Int32.make(0x27B7, 0x0A85),Int32.make(0x2E1B, 0x2138),Int32.make(0x4D2C, 0x6DFC),
			Int32.make(0x5338, 0x0D13),Int32.make(0x650A, 0x7354),Int32.make(0x766A, 0x0ABB),Int32.make(0x81C2, 0xC92E),Int32.make(0x9272, 0x2C85),
			Int32.make(0xA2BF, 0xE8A1),Int32.make(0xA81A, 0x664B),Int32.make(0xC24B, 0x8B70),Int32.make(0xC76C, 0x51A3),Int32.make(0xD192, 0xE819),
			Int32.make(0xD699, 0x0624),Int32.make(0xF40E, 0x3585),Int32.make(0x106A, 0xA070),Int32.make(0x19A4, 0xC116),Int32.make(0x1E37, 0x6C08),
			Int32.make(0x2748, 0x774C),Int32.make(0x34B0, 0xBCB5),Int32.make(0x391C, 0x0CB3),Int32.make(0x4ED8, 0xAA4A),Int32.make(0x5B9C, 0xCA4F),
			Int32.make(0x682E, 0x6FF3),Int32.make(0x748F, 0x82EE),Int32.make(0x78A5, 0x636F),Int32.make(0x84C8, 0x7814),Int32.make(0x8CC7, 0x0208),
			Int32.make(0x90BE, 0xFFFA),Int32.make(0xA450, 0x6CEB),Int32.make(0xBEF9, 0xA3F7),Int32.make(0xC671, 0x78F2)
		];
		var HASH : Array<Int32> = [
			Int32.make(0x6A09, 0xE667),Int32.make(0xBB67, 0xAE85),Int32.make(0x3C6E, 0xF372),Int32.make(0xA54F, 0xF53A),
			Int32.make(0x510E, 0x527F),Int32.make(0x9B05, 0x688C),Int32.make(0x1F83, 0xD9AB),Int32.make(0x5BE0, 0xCD19)
		];
		#end

		var W = new Array<Int32>();
		W[64] = #if !neko 0 #else Int32.make(0, 0) #end;
		var a:Int32,b:Int32,c:Int32,d:Int32,e:Int32,f:Int32,g:Int32,h:Int32;
		var T1, T2;
		/* append padding */
		#if !neko
		m[l >> 5] |= 0x80 << (24 - l % 32);
		m[((l + 64 >> 9) << 4) + 15] = l;
		#else
		if (m[l >> 5] == null) m[l >> 5] = Int32.make(0, 0);
		m[l >> 5] = Int32.or(m[l >> 5], Int32.shl(Int32.make(0, 0x80), (24 - l % 32)));
		m[((l + 64 >> 9) << 4) + 15] = Int32.make(0, l);
		#end
		var i : Int = 0;
		while ( i < m.length ) {
			a = HASH[0]; b = HASH[1]; c = HASH[2]; d = HASH[3]; e = HASH[4]; f = HASH[5]; g = HASH[6]; h = HASH[7];
			for ( j in 0...64 ) {
				if (j < 16)
					W[j] = m[j + i];
				else
					W[j] = Util.safeAdd(Util.safeAdd(Util.safeAdd(Gamma1256(W[j - 2]), W[j - 7]), Gamma0256(W[j - 15])), W[j - 16]);
				T1 = Util.safeAdd(Util.safeAdd(Util.safeAdd(Util.safeAdd(h, Sigma1256(e)), Ch(e, f, g)), K[j]), W[j]);
				T2 = Util.safeAdd(Sigma0256(a), Maj(a, b, c));
				h = g; g = f; f = e; e = Util.safeAdd(d, T1); d = c; c = b; b = a; a = Util.safeAdd(T1, T2);
			}
			HASH[0] = Util.safeAdd(a, HASH[0]);
			HASH[1] = Util.safeAdd(b, HASH[1]);
			HASH[2] = Util.safeAdd(c, HASH[2]);
			HASH[3] = Util.safeAdd(d, HASH[3]);
			HASH[4] = Util.safeAdd(e, HASH[4]);
			HASH[5] = Util.safeAdd(f, HASH[5]);
			HASH[6] = Util.safeAdd(g, HASH[6]);
			HASH[7] = Util.safeAdd(h, HASH[7]);
			i += 16;
		}
		return HASH;
	}
/*#else
	public static function encode(s : Bytes) : Bytes {
		var _ctx : Void = sha_init(256);
		sha_update(_ctx, s.getData());
		return Bytes.ofData(sha_final(_ctx));
	}

	private static var sha_init = neko.Lib.load("hash","sha_init",1);
	private static var sha_update = neko.Lib.load("hash","sha_update",2);
	private static var sha_final = neko.Lib.load("hash","sha_final",1);
#end*/

}
