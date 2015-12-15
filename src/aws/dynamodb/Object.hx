package aws.dynamodb;

/**
 * A DynamoDB record.
 * 
 * @author Sam MacPherson
 */
@:skip
#if !macro @:autoBuild(aws.dynamodb.RecordMacros.macroBuild()) #end
class Object #if (!js && !macro) extends sys.db.Object #end {
	
	#if js
	var _manager(default,never) : aws.dynamodb.Manager<Dynamic>;
	#end

	public function new () {
		#if (!js && !macro)
		super();
		#end
		
		#if !macro
		if ( _manager == null ) untyped _manager = Type.getClass(this).manager;
		#end
	}
	
	#if !macro
	#if js
	public function insert ():promhx.Promise<Dynamic> {
		return untyped _manager.doInsert(this);
	}

	public function update ():promhx.Promise<Dynamic> {
		return untyped _manager.doUpdate(this);
	}

	public function delete ():promhx.Promise<Dynamic> {
		return untyped _manager.doDelete(this);
	}

	public function isLocked () {
		throw "Lock does not apply for DynamoDB.";
	}

	public function toString () : String {
		return untyped _manager.objectToString(this);
	}
	
	public function lock ():Void {
		throw "Lock does not apply for DynamoDB.";
	}
	#else
	public override function lock ():Void {
		throw "Lock does not apply for DynamoDB.";
	}
	#end
	
	public function put () : #if js promhx.Promise<Dynamic> #else Void #end {
		return untyped _manager.doPut(this);
	}
	#end
	
	public macro function conditionalUpdate (ethis, expr:haxe.macro.Expr.ExprOf<Bool>) {
		return macro untyped $ethis._manager.doConditionalUpdate($ethis, ${ RecordMacros.buildCondition(expr) });
	}
	
}