package com.amazonaws.dynamodb;

/**
 * Interface for automated throughput regulation.
 * If included in a database configuration then the database will send metrics to the implementor.
 * 
 * @author Sam MacPherson
 */

interface ThroughputRegulator {
	
	function init (database:Database):Void;
	function shutdown ():Void;
	function pause ():Void;
	function resume ():Void;
	function readConsumed (table:String, units:Int):Void;
	function writeConsumed (table:String, units:Int):Void;
	function readFailed (table:String):Void;
	function writeFailed (table:String):Void;
	
}