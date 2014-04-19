package aws.dynamodb;

import aws.dynamodb.RecordInfos;
import haxe.macro.Expr;
import haxe.macro.Type;
#if macro
import haxe.macro.Context;
#end

using Lambda;
using StringTools;

class RecordMacros {
	
	static function exprToString (expr:Expr):String {
		var p = Context.currentPos();
		
		return switch (expr.expr) {
			case EConst(c):
				switch (c) {
					case CIdent(s), CString(s):
						return s;
					default:
						throw "Metadata should be identifier.";
				}
			default:
				throw "Metadata should be identifier.";
		};
	}
	
	static function getRecordIndex (meta:MetaAccess, expr:Expr):{ name:String, type:RecordType } {
		var name = exprToString(expr);
		var type = null;
		for (i in meta.get()) {
			if (i.name == ":type_" + i.name) {
				switch (i.params[0].expr) {
					case EConst(c):
						switch (c) {
							case CIdent(s):
								type = std.Type.createEnum(RecordType, s);
							default:
								throw "Invalid type.";
						}
					default:
						throw "Invalid type.";
				}
				
				break;
			}
		}
		return { name:name, type:type };
	}
	
	static function metaToInfos (meta:MetaAccess):RecordInfos {
		var obj:RecordInfos = { table:null, primaryIndex:null, indexes:[], fields:[] };
		
		for (i in meta.get()) {
			switch (i.name) {
				case ":prefix": obj.prefix = exprToString(i.params[0]);
				case ":table": obj.table = exprToString(i.params[0]);
				case ":shard": obj.shard = exprToString(i.params[0]);
				case ":id":
					var key:Dynamic = { };
					key.hash = getRecordIndex(meta, i.params[0]);
					if (i.params.length > 1) key.range = getRecordIndex(meta, i.params[1]);
					obj.primaryIndex = key;
				case ":sindex":
					var key:Dynamic = { };
					key.hash = getRecordIndex(meta, i.params[1]);
					if (i.params.length > 2) key.range = getRecordIndex(meta, i.params[2]);
					obj.indexes.push({name:exprToString(i.params[0]), index:key});
				default:
					if (i.name.startsWith(":type_")) {
						obj.fields.push({ name:i.name.substr(":type_".length), type:std.Type.createEnum(RecordType, exprToString(i.params[0])) });
					}
			}
		}
		
		return obj;
	}
	
	public static function macroBuild ():Array<Field> {
		var cls = Context.getLocalClass().get();
		var fields = Context.getBuildFields();
		var p = Context.currentPos();
		
		for (i in fields) {
			if (i.name == "manager") {
				switch (i.kind) {
					case FVar(_, e), FProp(_, _, _, e):
						switch (Context.typeof(e)) {
							case TInst(t, _):
								for (o in cls.meta.get()) {
									t.get().meta.add(o.name, o.params, p);
								}
							default:
						}
					default:
				}
			} else {
				var type = null;
				
				switch (i.kind) {
					case FVar(t, _), FProp(_, _, t, _) if (!i.meta.exists(function (e) { return e.name == ":skip"; } )):
						switch (t) {
							case TPath(p):
								switch (p.name) {
									case "String": type = macro DString;
									case "Int": type = macro DInt;
									case "Float": type = macro DFloat;
									case "Bool": type = macro DBool;
									case "Date": type = macro DDate;
									default:
										Context.error("Invalid type.", i.pos);
								}
							default:
								Context.error("Invalid type.", i.pos);
						}
					default:
				}
				
				if (type != null) cls.meta.add(":type_" + i.name, [type], p);
			}
		}
		
		var infos = metaToInfos(cls.meta);
		fields.push( { name:"__dynamodb_infos", meta:[], access:[AStatic], pos:p, kind:FVar(null, 
			Context.makeExpr(infos, p)
		) });
		
		return fields;
	}
	
	public static function macroGet( em : Expr, id : Expr, consistent : Expr ) {
		var infos = getInfos(Context.typeof(em));
		
		return switch (id.expr) {
			case EObjectDecl(a):
				for (i in a) {
					checkType(Context.typeof(i.expr), i.field, infos, id.pos);
				}
				
				macro $em.unsafeGetWithKeys($id, $consistent);
			default:
				checkType(Context.typeof(id), infos.primaryIndex.hash.name, infos, id.pos);
				
				macro $em.unsafeGet($id, $consistent);
		}
	}
	
	public static function macroSearch( em : Expr, econd : Expr, eopt : Expr, econsistent : Expr, ?single ) {
		// allow both search(e,opts) and search(e,lock)
		if( eopt != null && (econsistent == null || std.Type.enumEq(econsistent.expr, EConst(CIdent("null")))) ) {
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
	
	static function getInfos (t:haxe.macro.Type):RecordInfos {
		switch (t) {
			case TInst(t, _):
				return metaToInfos(t.get().meta);
			default:
				throw "Bad type.";
		}
	}
	
	static function buildQuery( em : Expr, econd : Expr, ?eopt : Expr ) {
		var p = Context.currentPos();
		var query = new Array<{field:String, expr:Expr}>();
		var infos = getInfos(Context.typeof(em));
		var rangeKey = null;
		
		if ( eopt != null && !std.Type.enumEq(eopt.expr, EConst(CIdent("null"))) ) {
			var opt = buildOptions(eopt, infos);
			if (opt.orderBy != null) {
				rangeKey = exprToString(opt.orderBy.field);
				query.push({field:"ScanIndexForward", expr:opt.orderBy.asc});
			}
			if( opt.limit != null ) {
				query.push({field:"Limit", expr:opt.limit.len});
				if ( opt.limit.pos != null ) {
					query.push({field:"ExclusiveStartKey", expr:opt.limit.pos});
				}
			}
		}
		
		var condResult = buildCond(econd, infos);
		query.push( { field:"KeyConditions", expr:condResult.expr } );
		if (rangeKey != condResult.range && rangeKey != null) Context.error("orderBy field must match the range field in the conditional.", eopt.pos);
		if (condResult.index != null) query.push( { field:"IndexName", expr:Context.makeExpr(condResult.index, p) } );
		
		return { expr:EObjectDecl(query), pos:p };
	}
	
	static function buildOptions( eopt : Expr, infos:RecordInfos ) {
		var p = eopt.pos;
		var opt = { limit : null, orderBy : null };
		switch( eopt.expr ) {
		case EObjectDecl(fields):
			var limit = null;
			for( o in fields ) {
				switch( o.field ) {
				case "orderBy":
					opt.orderBy = buildOrderBy(o.expr, infos, o.expr.pos);
				default:
				}
			}
			for( o in fields ) {
				switch( o.field ) {
				case "limit":
					opt.limit = buildLimit(o.expr, opt.orderBy, infos, o.expr.pos);
				default:
				}
			}
		default:
			Context.error("Options should be { orderBy : field, limit : [a,b] }", p);
		}
		return opt;
	}
	
	static function buildLimit (limit:Expr, orderBy:{ field:Expr, asc:Expr }, infos:RecordInfos, p):{ ?pos:Expr, len:Expr } {
		switch (limit.expr) {
			case EConst(c):
				return { len:limit };
			case EArrayDecl(a):
				checkType(Context.typeof(a[0]), orderBy.field != null ? exprToString(orderBy.field) : infos.primaryIndex.range.name, infos, p);
				
				return { pos:a[0], len:a[1] };
			default:
				Context.error("Bad limit.", p);
		}
		
		return null;
	}
	
	static function buildOrderBy (orderBy:Expr, infos:RecordInfos, p):{ field:Expr, asc:Expr } {
		switch (orderBy.expr) {
			case EConst(c):
				switch (c) {
					case CIdent(s):
						if (s == infos.primaryIndex.range.name || infos.indexes.exists(function (e) { return s == e.index.range.name; } )) {
							return { field:Context.makeExpr(s, p), asc:macro true };
						} else {
							Context.error("orderBy field must be a range key for some index.", p);
						}
					default:
						Context.error("Bad orderBy expression.", p);
				}
			case EUnop(op, postFix, e) if (!postFix && op == OpNeg):
				return { field:buildOrderBy(e, infos, p).field, asc:macro false };
			default:
				Context.error("Bad orderBy expression.", p);
		}
		
		return null;
	}
	
	static function buildType (field:String, v:Expr, infos:RecordInfos):Expr {
		return { expr:EObjectDecl([{
			field: switch (getFieldType(infos, field)) {
				case DString: "S";
				case DFloat, DInt:
					//Need to convert number to string
					v = macro Std.string($v);
					"N";
				case DDate:
					v = macro Std.string($v.getTime());
					"N";
				case DBool:
					v = macro $v ? "1" : "0";
					"N";
			},
			expr: v
		}]), pos:v.pos};
	}
	
	static function buildComp (field:String, v:Expr, infos:RecordInfos, op:String):Expr {
		checkType(Context.typeof(v), field, infos, v.pos);
		
		var fields = new Array<{field:String, expr:Expr}>();
		
		fields.push( { field:"ComparisonOperator", expr:Context.makeExpr(op, v.pos) } );
		fields.push( { field:"AttributeValueList", expr: { expr:EArrayDecl([
			buildType(field, v, infos)
		]), pos:v.pos } } );
		
		return { expr:EObjectDecl(fields), pos:v.pos };
	}
	
	static function buildBinOp (fields:Array<{field:String, expr:Expr}>, infos:RecordInfos, op:Binop, e1:Expr, e2:Expr, p):Void {
		var comp = null;
		
		switch (op) {
			case OpBoolAnd:
				switch (e1.expr) {
					case EBinop(op, e1, e2):
						buildBinOp(fields, infos, op, e1, e2, e1.pos);
					default:
						Context.error("Bad condition. Must be AND-delimited simple comparison on range field.", p);
						return;
				}
				switch (e2.expr) {
					case EBinop(op, e1, e2):
						buildBinOp(fields, infos, op, e1, e2, e2.pos);
					default:
						Context.error("Bad condition. Must be AND-delimited simple comparison on range field.", p);
						return;
				}
				return;
			case OpGt:
				comp = "GT";
			case OpGte:
				comp = "GE";
			case OpLt:
				comp = "LT";
			case OpLte:
				comp = "LE";
			case OpEq:
				comp = "EQ";
			default:
				Context.error("Bad condition. Must be AND-delimited simple comparison on table/index fields.", p);
				return;
		}
		
		var field = null;
		var expr = null;
		
		switch (e1.expr) {
			case EConst(CIdent(s)) if (s.charAt(0) == "$"):
				field = s.substr(1);
			default:
				expr = e1;
		}
		switch (e2.expr) {
			case EConst(CIdent(s)) if (s.charAt(0) == "$"):
				field = s.substr(1);
			default:
				expr = e2;
		}
		
		if (field == null || expr == null) {
			Context.error("Comparison must be a simple comparison on table/index fields.", p);
			return;
		}
		
		fields.push( { field:field, expr:buildComp(field, expr, infos, comp) } );
	}
	
	static function isEq (expr:Expr):Bool {
		switch (expr.expr) {
			case EObjectDecl(f):
				for (i in f) {
					if (i.field == "ComparisonOperator") {
						return exprToString(i.expr) == "EQ";
					}
				}
			default:
		}
		
		return false;
	}
	
	static function buildCond (cond:Expr, infos:RecordInfos):{expr:Expr, range:String, ?index:String} {
		var p = cond.pos;
		var fields = new Array<{field:String, expr:Expr}>();
		var hash = null;
		var range = null;
		var index = null;
		
		switch (cond.expr) {
			case EObjectDecl(f):
				for (i in f) {
					fields.push( { field:i.field, expr:buildComp(i.field, i.expr, infos, "EQ") } );
				}
			case EBinop(op, e1, e2):
				buildBinOp(fields, infos, op, e1, e2, cond.pos);
			default:
				Context.error("Bad condition. Must be AND-delimited simple comparison on table/index fields.", p);
		}
		
		for (i in fields) {
			if (isEq(i.expr)) {
				hash = i.field;
			} else {
				range = i.field;
			}
		}
		
		if (infos.primaryIndex.hash.name != hash || infos.primaryIndex.range.name != range) {
			for (i in infos.indexes) {
				if (i.index.hash.name == hash && i.index.range.name == range) {
					index = i.name;
					break;
				}
			}
			
			if (index == null) Context.error("Could not match condition to an index.", cond.pos);
		}
		
		return {expr:{ expr:EObjectDecl(fields), pos:p }, range:range, index:index };
	}
	
	static function checkType (type:Type, field:String, infos:RecordInfos, p):Void {
		var failure = false;
		var rt = getFieldType(infos, field);
		if (rt == null) Context.error("Unknown field.", p);
		
		switch (type) {
			case TInst(t, _):
				switch (t.toString()) {
					case "String": failure = rt != DString;
					case "Date": failure = rt != DDate;
					default:
				}
			case TAbstract(a, _):
				switch (a.toString()) {
					case "Int": failure = rt != DInt;
					case "Float": failure = rt != DFloat;
					case "Bool": failure = rt != DBool;
					default:
				}
			default:
		}
		
		if (failure) Context.error("Type mismatch.", p);
	}
	
	static function getFieldType (infos:RecordInfos, field:String):RecordType {
		for (i in infos.fields) {
			if (i.name == field) {
				return i.type;
			}
		}
		
		return null;
	}
	
}