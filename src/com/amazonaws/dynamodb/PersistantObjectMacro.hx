package com.amazonaws.dynamodb;

/**
 * Will attach additional type information to all subclasses of persistant object.
 * @author Sam MacPherson
 */

import haxe.macro.Context;
import haxe.macro.Expr;
import haxe.macro.Type;

class PersistantObjectMacro {

	@:macro public static function build ():Array<Field> {
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