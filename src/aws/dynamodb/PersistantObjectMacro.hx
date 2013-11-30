/****
* Copyright (C) 2013 Sam MacPherson
* 
* Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
* 
* The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
****/



package aws.dynamodb;

/**
 * Will attach additional type information to all subclasses of persistant object.
 * @author Sam MacPherson
 */

import haxe.macro.Context;
import haxe.macro.Expr;
import haxe.macro.Type;

class PersistantObjectMacro {

	macro public static function build ():Array<Field> {
		var fields = Context.getBuildFields();
		
		for (i in fields) {
			var name = getFieldType(i.kind);
			if (name != null) i.meta.push({name:"type", params:[Context.makeExpr(name, i.pos)], pos:i.pos});
		}
		
		return fields;
	}
	
	static function getFieldType (f:FieldType):String {
		return switch (f) {
			case FVar(t, e):
				switch (t) {
					case TPath(p):
						p.name;
					default:
						null;
				}
			default:
				null;
		}
	}
	
}