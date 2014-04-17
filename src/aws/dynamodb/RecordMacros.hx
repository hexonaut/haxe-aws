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
	
	public static function macroSearch( em : Expr, econd : Expr, eopt : Expr, econsistent : Expr, ?single ) {
		// allow both search(e,opts) and search(e,lock)
		if( eopt != null && (econsistent == null || Type.enumEq(econsistent.expr, EConst(CIdent("null")))) ) {
			switch( eopt.expr ) {
			case EObjectDecl(_):
			default:
				var tmp = eopt;
				eopt = econsistent;
				econsistent = tmp;
			}
		}
		var query = buildQuery(em, econd, eopt);
		var pos = Context.currentPos();
		var e = { expr : ECall( { expr : EField(em, "unsafeObjects"), pos : pos }, [query,defaultTrue(econsistent)]), pos : pos };
		if( single )
			e = { expr : ECall( { expr : EField(e, "first"), pos : pos }, []), pos : pos };
		return e;
	}
	
	static function defaultTrue( e : Expr ) {
		return switch( e.expr ) {
		case EConst(CIdent("null")): { expr : EConst(CIdent("true")), pos : e.pos };
		default: e;
		}
	}
	
	static function buildQuery( em : Expr, econd : Expr, ?eopt : Expr ) {
		var p = Context.currentPos();
		var query = new Array<{field:String, expr:Expr}>();
		
		if ( eopt != null && !Type.enumEq(eopt.expr, EConst(CIdent("null"))) ) {
			var opt = buildOptions(eopt);
			if( opt.orderBy != null ) {
				query.push({field:"IndexName", expr:opt.orderBy.field});
				query.push({field:"ScanIndexForward", expr:opt.orderBy.asc});
			}
			if( opt.limit != null ) {
				query.push({field:"Limit", expr:opt.limit.len});
				if ( opt.limit.pos != null ) {
					query.push({field:"ExclusiveStartKey", expr:opt.limit.pos});
				}
			}
		}
		
		return { expr:EObjectDecl(query), pos:p };
	}
	
	static function buildOptions( eopt : Expr ) {
		var p = eopt.pos;
		var opts = new haxe.ds.StringMap();
		var opt = { limit : null, orderBy : null };
		switch( eopt.expr ) {
		case EObjectDecl(fields):
			var limit = null;
			for( o in fields ) {
				if( opts.exists(o.field) ) Context.error("Duplicate option " + o.field, p);
				opts.set(o.field, true);
				switch( o.field ) {
				case "orderBy":
					opt.orderBy = buildOrderBy(o.expr, p);
				case "limit":
					opt.limit = buildLimit(o.expr, p);
				default:
					Context.error("Unknown option '" + o.field + "'", p);
				}
			}
		default:
			Context.error("Options should be { orderBy : field, limit : [a,b] }", p);
		}
		return opt;
	}
	
	static function buildLimit (limit:Expr, p):{ ?pos:Expr, len:Expr } {
		switch (limit.expr) {
			case EConst(c):
				return { len:limit };
			case EArrayDecl(a):
				return { pos:a[0], len:a[1] };
			default:
				Context.error("Unknown limit", p);
		}
		
		return null;
	}
	
	static function buildOrderBy (orderBy:Expr, p):{ field:Expr, asc:Expr } {
		switch (orderBy.expr) {
			case EConst(c):
				switch (c) {
					case CIdent(s):
						return { field:Context.makeExpr(s, p), asc:macro true };
					default:
						Context.error("Bad orderBy expression", p);
				}
			case EUnop(op, postFix, e) if (!postFix && op == OpNeg):
				return { field:buildOrderBy(e, p).field, asc:macro false };
			default:
				Context.error("Bad orderBy expression", p);
		}
		
		return null;
	}
	
}