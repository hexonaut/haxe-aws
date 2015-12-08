package aws.dynamodb;

/**
 * A DynamoDB record.
 * 
 * @author Sam MacPherson
 */
@:skip
#if !macro @:autoBuild(aws.dynamodb.RecordMacros.macroBuild()) #end
class Object #if !js extends sys.db.Object #end {
	
	#if js
	var _manager(default,never) : aws.dynamodb.Manager<Dynamic>;
	@:keep var __cache__:Dynamic;
	#end

	public function new () {
		#if !js
		super();
		#end
		
		if ( _manager == null ) untyped _manager = Type.getClass(this).manager;
	}
	
	#if js
	public function insert () {
		return untyped _manager.doInsert(this);
	}

	public function update () {
		return untyped _manager.doUpdate(this);
	}

	public function delete () {
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
	
	public function put ():Void {
		return untyped _manager.doPut(this);
	}
	
}