package aws.dynamodb;

/**
 * A DynamoDB record.
 * 
 * @author Sam MacPherson
 */
@:skip
#if !macro @:autoBuild(aws.dynamodb.RecordMacros.macroBuild()) #end
class Object extends sys.db.Object {

	public function new () {
		super();
		
		if ( _manager == null ) untyped _manager = Type.getClass(this).manager;
	}
	
	public function put ():Void {
		untyped _manager.doPut(this);
	}
	
	public override function lock ():Void {
		throw "Lock does not apply for DynamoDB.";
	}
	
}