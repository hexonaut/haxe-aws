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
						throw "Expr should be identifier or string.";
				}
			default:
				throw "Expr should be identifier or string.";
		};
	}
	
	static function exprToInt (expr:Expr):Int {
		var p = Context.currentPos();
		return switch (expr.expr) {
			case EConst(c):
				switch (c) {
					case CInt(s):
						return Std.parseInt(s);
					default:
						throw "Expr should be integer.";
				}
			default:
				throw "Expr should be integer.";
		};
	}
	
	static function metaToInfos (meta:MetaAccess):RecordInfos {
		var obj:RecordInfos = { table:null, primaryIndex:null, indexes:[], fields:[] };
		
		for (i in meta.get()) {
			switch (i.name) {
				case ":prefix": obj.prefix = exprToString(i.params[0]);
				case ":table": obj.table = exprToString(i.params[0]);
				case ":shard": obj.shard = exprToString(i.params[0]);
				case ":read": obj.readCap = exprToInt(i.params[0]);
				case ":write": obj.writeCap = exprToInt(i.params[0]);
				case ":id":
					var key:Dynamic = { };
					key.hash = exprToString(i.params[0]);
					if (i.params.length > 1) key.range = exprToString(i.params[1]);
					obj.primaryIndex = key;
				case ":lindex":
					var key:Dynamic = { };
					key.hash = exprToString(i.params[1]);
					if (i.params.length > 2) key.range = exprToString(i.params[2]);
					obj.indexes.push({name:exprToString(i.params[0]), index:key, global:false});
				case ":gindex":
					var key:Dynamic = { };
					var readCap:Int = null;
					var writeCap:Int = null;
					key.hash = exprToString(i.params[1]);
					if (i.params.length > 2) key.range = exprToString(i.params[2]);
					if (i.params.length > 3) readCap = exprToInt(i.params[3]);
					if (i.params.length > 4) writeCap = exprToInt(i.params[4]);
					obj.indexes.push({name:exprToString(i.params[0]), index:key, global:true, readCap:readCap, writeCap:writeCap});
				default:
					if (i.name.startsWith(":type_")) {
						var name = null;
						var params = null;
						switch (i.params[0].expr) {
							case EConst(c):
								name = exprToString(i.params[0]);
							case ECall(e, p):
								name = exprToString(e);
								params = p.map(function (e) { return null; } );
							default:
								throw "Invalid type.";
						}
						
						obj.fields.push({ name:i.name.substr(":type_".length), type:std.Type.createEnum(RecordType, name, params) });
					}
			}
		}
		
		return obj;
	}
	
	static function fillTypes (meta:MetaAccess, e:Expr):Expr {
		switch (e.expr) {
			case EObjectDecl(f):
				for (i in f) {
					if (i.field == "fields") {
						switch (i.expr.expr) {
							case EArrayDecl(a):
								for (o in a) {
									switch (o.expr) {
										case EObjectDecl(f):
											var name = null;
											for (i in f) {
												if (i.field == "name") {
													name = exprToString(i.expr);
												}
											}
											for (i in f) {
												if (i.field == "type") {
													switch (i.expr.expr) {
														case ECall(e, p):
															for (m in meta.get()) {
																if (m.name == ":type_" + name) {
																	switch (m.params[0].expr) {
																		case ECall(_, params):
																			p[0] = params[0];
																		default:
																	}
																	
																	break;
																}
															}
														default:
													}
													
													break;
												}
											}
										default:
									}
								}
							default:
						}
						
						break;
					}
				}
			default:
		}
		
		return e;
	}
	
	static function complexTypeToRecordTypeExpr (t:ComplexType, pos):Expr {
		return switch (t) {
			case TPath(p):
				switch (p.name) {
					case "String", "SString": macro DString;
					case "Int", "SInt": macro DInt;
					case "Float", "SFloat": macro DFloat;
					case "Bool", "SBool": macro DBool;
					case "SDate": macro DDate;
					case "Date", "SDateTime": macro DDateTime;
					case "STimeStamp": macro DTimeStamp;
					case "SBinary", "Bytes": macro DBinary;
					case "SEnum": macro DEnum(${
						switch (p.params[0]) {
							case TPType(ct):
								{ expr:EConst(CIdent(switch (ct) { case TPath(tp): tp.name; default: Context.error("Invalid type.", pos); ""; } )), pos:pos }
							case TPExpr(e):
								e;
							}
						});
					case "SSet": macro DSet(${complexTypeToRecordTypeExpr(switch (p.params[0]) {
							case TPType(ct): ct;
							case TPExpr(e): Context.error("Invalid type.", e.pos);
						}, pos)});
					default:
						var type = Context.getType((p.pack.length > 0 ? p.pack.join(".") : "") + p.name);
						if (type != null) {
							switch (type) {
								case TAbstract(t, _):
									return complexTypeToRecordTypeExpr(Context.toComplexType(t.get().type), pos);
								default:
							}
						}
						
						Context.error("Invalid type.", pos);
					}
				default:
					Context.error("Invalid type.", pos);
		}
	}
	
	static function buildField( f : Field, fields : Array<Field> ) {
		var ft = switch (f.kind) {
			case FVar(t, _), FProp(_, _, t, _): t;
			default: return;
		}
		var p = switch( ft ) {
		case TPath(p): p;
		default: return;
		}
		var pos = f.pos;
		switch( p.name ) {
		case "STimeStamp":
			f.kind = FProp("dynamic", "dynamic", ft, null);
			f.meta.push( { name : ":isVar", params : [], pos : f.pos } );
			f.meta.push( { name : ":data", params : [], pos : f.pos } );
			var meta = [ { name : ":hide", params : [], pos : pos } ];
			var efield = { expr : EConst(CIdent(f.name)), pos : pos };
			var get = {
				args : [],
				params : [],
				ret : ft,
				expr : macro return $efield == null ? null : Date.fromTime(cast $efield),
			};
			var set = {
				args : [{ name : "_v", opt : false, type : ft, value : null }],
				params : [],
				ret : ft,
				expr : macro { $efield = _v == null ? null : cast _v.getTime() + Math.random()*1000; return _v; },
			};
			fields.push( { name : "get_" + f.name, pos : pos, meta : meta, access : [APrivate], doc : null, kind : FFun(get) } );
			fields.push( { name : "set_" + f.name, pos : pos, meta : meta, access : [APrivate], doc : null, kind : FFun(set) } );
		}
	}
	
	public static function macroBuild ():Array<Field> {
		var cls = Context.getLocalClass().get();
		var fields = Context.getBuildFields();
		var p = Context.currentPos();
		
		//Prevent sys.db.Object autobuild macro from building db infos
		cls.meta.add(":skip", [], p);
		
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
						type = complexTypeToRecordTypeExpr(t, i.pos);
					default:
				}
				
				if (type != null) cls.meta.add(":type_" + i.name, [type], p);
				
				//Build complex fields
				buildField(i, fields);
			}
		}
		
		var infos = metaToInfos(cls.meta);
		fields.push( { name:"__dynamodb_infos", meta:[], access:[AStatic], pos:p, kind:FVar(null, 
			fillTypes(cls.meta, Context.makeExpr(infos, p))
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
				checkType(Context.typeof(id), infos.primaryIndex.hash, infos, id.pos);
				
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
		
		var condResult = buildCond(em, econd, infos, rangeKey);
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
				checkType(Context.typeof(a[0]), orderBy.field != null ? exprToString(orderBy.field) : infos.primaryIndex.range, infos, p);
				
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
						if (s == infos.primaryIndex.range || infos.indexes.exists(function (e) { return s == e.index.range; } )) {
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
	
	static function buildType (em:Expr, field:String, v:Expr, infos:RecordInfos):Expr {
		var fname = switch (getFieldType(infos, field)) {
			case DString: "S";
			case DBinary: "B";
			case DSet(t):
				switch (t) {
					case DString: "SS";
					case DBinary: "BS";
					default: "NS";
				}
			default: "N";
		};
		v = macro Reflect.field($em.haxeToDynamo(${Context.makeExpr(field, v.pos)}, $v), ${Context.makeExpr(fname, v.pos)});
		return { expr:EObjectDecl([{
			field: fname,
			expr: v
		}]), pos:v.pos};
	}
	
	static function buildComp (em:Expr, field:String, v:Expr, infos:RecordInfos, op:String):Expr {
		checkType(Context.typeof(v), field, infos, v.pos);
		
		var fields = new Array<{field:String, expr:Expr}>();
		
		fields.push( { field:"ComparisonOperator", expr:Context.makeExpr(op, v.pos) } );
		fields.push( { field:"AttributeValueList", expr: { expr:EArrayDecl([
			buildType(em, field, v, infos)
		]), pos:v.pos } } );
		
		return { expr:EObjectDecl(fields), pos:v.pos };
	}
	
	static function buildBinOp (em:Expr, fields:Array<{field:String, expr:Expr}>, infos:RecordInfos, op:Binop, e1:Expr, e2:Expr, p):Void {
		var comp = null;
		
		switch (op) {
			case OpBoolAnd:
				switch (e1.expr) {
					case EBinop(op, e1, e2):
						buildBinOp(em, fields, infos, op, e1, e2, e1.pos);
					default:
						Context.error("Bad condition. Must be AND-delimited simple comparison on range field.", p);
						return;
				}
				switch (e2.expr) {
					case EBinop(op, e1, e2):
						buildBinOp(em, fields, infos, op, e1, e2, e2.pos);
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
		
		fields.push( { field:field, expr:buildComp(em, field, expr, infos, comp) } );
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
	
	static function buildCond (em:Expr, cond:Expr, infos:RecordInfos, ?orderBy:String):{expr:Expr, range:String, ?index:String} {
		var p = cond.pos;
		var fields = new Array<{field:String, expr:Expr}>();
		var hash = null;
		var range = null;
		var index = null;
		
		switch (cond.expr) {
			case EObjectDecl(f):
				for (i in f) {
					fields.push( { field:i.field, expr:buildComp(em, i.field, i.expr, infos, "EQ") } );
				}
			case EBinop(op, e1, e2):
				buildBinOp(em, fields, infos, op, e1, e2, cond.pos);
			default:
				Context.error("Bad condition. Must be AND-delimited simple comparison on table/index fields.", p);
		}
		
		for (i in fields) {
			if (isEq(i.expr) && hash == null) {
				hash = i.field;
			} else {
				range = i.field;
			}
		}
		
		//Pull range info from orderBy clause
		if (range == null) range = orderBy;
		
		if (infos.primaryIndex.hash != hash || infos.primaryIndex.range != range) {
			for (i in infos.indexes) {
				if (i.index.hash == hash && i.index.range == range) {
					index = i.name;
					break;
				}
			}
			
			if (index == null) {
				//No exact match -- if range is null then try partial match
				if (range == null) {
					if (infos.primaryIndex.hash != hash) {
						for (i in infos.indexes) {
							if (i.index.hash == hash) {
								index = i.name;
								break;
							}
						}
						
						if (index == null) Context.error("Could not match condition to an index.", cond.pos);
					}
				} else {
					Context.error("Could not match condition to an index.", cond.pos);
				}
			}
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