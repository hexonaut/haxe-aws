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
	
}