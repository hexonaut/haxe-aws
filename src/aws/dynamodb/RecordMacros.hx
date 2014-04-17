package aws.dynamodb;

import sys.db.RecordInfos;
import haxe.macro.Expr;
import haxe.macro.Type.VarAccess;
#if macro
import haxe.macro.Context;
#end

class RecordMacros {
	
	static function identToStr (expr:Expr):Expr {
		var p = Context.currentPos();
		
		return switch (expr.expr) {
			case EConst(c):
				switch (c) {
					case CIdent(s):
						return { expr: EConst(CString(s)), pos:p };
					default:
						throw "Metadata should be identifier.";
				}
			default:
				throw "Metadata should be identifier.";
		};
	}
	
	public static function macroBuild ():Array<Field> {
		var cls = Context.getLocalClass().get();
		var fields = Context.getBuildFields();
		var p = Context.currentPos();
		
		var prefix = null;
		var table = null;
		var shard = null;
		var hash = null;
		var range = null;
		
		for (i in cls.meta.get()) {
			switch (i.name) {
				case ":prefix": prefix = i.params[0];
				case ":table": table = i.params[0];
				case ":shard": shard = i.params[0];
				case ":id":
					hash = identToStr(i.params[0]);
					if (i.params.length > 1) range = identToStr(i.params[1]);
				default:
			}
		}
		
		fields.push( { name:"__dynamodb_infos", meta:[], access:[AStatic], pos:p, kind:FVar(null, 
			macro { prefix:$prefix, table:$table, shard:$shard, hash:$hash, range:$range }
		) });
		
		return fields;
	}
	
}