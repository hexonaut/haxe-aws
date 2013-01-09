package com.amazonaws.dynamodb;

import haxe.Timer;
#if neko
import neko.vm.Mutex;
import neko.vm.Thread;
#end

using DateTools;

private typedef TableMetric = {
	readErrors:Int,
	readSuccesses:Int,
	writeErrors:Int,
	writeSuccesses:Int,
	lastFailedReadUpper:Date,
	lastFailedReadLower:Date,
	lastFailedWriteUpper:Date,
	lastFailedWriteLower:Date
};

/**
 * Provides throughput regulation by exponentially stepping up the throughput limit when the error rate reaches a certain threshold.
 * Runs a seperate thread which executes roughly every 5 seconds to watch over the datastream. This class is completely thread safe.
 * 
 * @author Sam MacPherson
 */

class ExponentialStepThroughputRegulator implements ThroughputRegulator {
	
	var db:Database;
	
	/**
	 * The mimimum read capacity units.
	 */
	public var minRead(default, setMinRead):Int;
	
	/**
	 * The maximum read capacity units.
	 */
	public var maxRead(default, setMaxRead):Int;
	
	/**
	 * The mimimum write capacity units.
	 */
	public var minWrite(default, setMinWrite):Int;
	
	/**
	 * The maximum write capacity units.
	 */
	public var maxWrite(default, setMaxWrite):Int;
	
	/**
	 * The ratio of errors / total requests before increasing the limit.
	 * 
	 * Default is 0.01
	 */
	public var errorUpperTheshold(default, setErrorUpperThreshold):Float;
	
	/**
	 * The ratio of errors must be above the upper theshold for this long before the threshold is increased.
	 * 
	 * Default is 5 minutes
	 */
	public var upperThesholdTime(default, setMinUpdateDelay):Float;
	
	/**
	 * The ratio of errors / total requests before decreasing the limit.
	 * 
	 * Default is 0
	 */
	public var errorLowerThreshold(default, setErrorLowerThreshold):Float;
	
	/**
	 * The ratio of errors must be below the lower theshold for this long before the threshold is lowered.
	 * 
	 * Default is 1 hour
	 */
	public var lowerThesholdTime(default, setErrorLowerThreshold):Float;
	
	/**
	 * If true then the throughput regulator is paused.
	 */
	public var paused(default, null):Bool;
	
	var tableInfos:Hash<TableInfo>;
	var metrics:Hash<TableMetric>;
	#if neko
	var lock:Mutex;
	#else
	var timer:Timer;
	#end
	var doShutdown:Bool;

	public function new (?minRead:Int = 1, ?maxRead:Int = 1, ?minWrite:Int = 1, ?maxWrite:Int = 1) {
		this.minRead = minRead;
		this.maxRead = maxRead;
		this.minWrite = minWrite;
		this.maxRead = maxWrite;
		this.errorUpperTheshold = 0.01;
		this.upperThesholdTime = DateTools.minutes(5);
		this.errorLowerTheshold = 0;
		this.lowerThesholdTime = DateTools.hours(1);
		
		tableInfos = new Hash<TableInfo>();
		metrics = new Hash<TableMetric>();
		#if neko
		lock = new Mutex();
		#end
		doShutdown = false;
	}
	
	public function init (database:Database):Void {
		db = database;
		
		//Populate table info with initial data
		for (i in db.getAllTables()) {
			var delay = 1;
			while (true) {
				try {
					tableInfos.set(i, db.describeTable(i));
					metrics.set(i, { readErrors:0, readSuccesses:0, writeErrors:0, writeSuccesses:0, lastFailedReadLower:Date.now(), lastFailedReadUpper:Date.now(), lastFailedWriteLower:Date.now(), lastFailedWriteUpper:Date.now() });
				} catch (e:DynamoDBException) {
					if (delay > 64) throw "Failed to get all table descriptions.";	//Fail after 64 seconds
					
					Sys.sleep(delay);
					delay = delay << 1;
				}
			}
		}
		
		//Startup monitor thread
		#if neko
		Thread.create(tick);
		#else
		timer = new Timer(5000);
		timer.run = tick;
		#end
	}
	
	public function shutdown ():Void {
		#if neko
		lock.acquire();
		doShutdown = true;
		lock.release();
		#else
		timer.stop();
		#end
	}
	
	function pause ():Void {
		#if neko
		lock.acquire();
		#end
		paused = true;
		#if neko
		lock.release();
		#end
	}
	
	function resume ():Void {
		#if neko
		lock.acquire();
		#end
		paused = false;
		#if neko
		lock.release();
		#end
	}
	
	function setMinRead (v:Int):Int {
		#if neko
		lock.acquire();
		#end
		minRead = v;
		#if neko
		lock.release();
		#end
		return v;
	}
	
	function setMaxRead (v:Int):Int {
		#if neko
		lock.acquire();
		#end
		maxRead = v;
		#if neko
		lock.release();
		#end
		return v;
	}
	
	function setMinWrite (v:Int):Int {
		#if neko
		lock.acquire();
		#end
		minWrite = v;
		#if neko
		lock.release();
		#end
		return v;
	}
	
	function setMaxWrite (v:Int):Int {
		#if neko
		lock.acquire();
		#end
		maxWrite = v;
		#if neko
		lock.release();
		#end
		return v;
	}
	
	function setErrorUpperTheshold (v:Float):Float {
		#if neko
		lock.acquire();
		#end
		errorUpperTheshold = v;
		#if neko
		lock.release();
		#end
		return v;
	}
	
	function setUpperThesholdTime (v:Float):Float {
		#if neko
		lock.acquire();
		#end
		upperThesholdTime = v;
		#if neko
		lock.release();
		#end
		return v;
	}
	
	function setErrorLowerTheshold (v:Float):Float {
		#if neko
		lock.acquire();
		#end
		errorLowerTheshold = v;
		#if neko
		lock.release();
		#end
		return v;
	}
	
	function setLowerThesholdTime (v:Float):Float {
		#if neko
		lock.acquire();
		#end
		lowerThesholdTime = v;
		#if neko
		lock.release();
		#end
		return v;
	}
	
	public function readConsumed (table:String, units:Int):Void {
		#if neko
		lock.acquire();
		#end
		metrics.get(table).readSuccesses++;
		#if neko
		lock.release();
		#end
	}
	
	public function writeConsumed (table:String, units:Int):Void {
		#if neko
		lock.acquire();
		#end
		metrics.get(table).writeSuccesses++;
		#if neko
		lock.release();
		#end
	}
	
	public function readFailed (table:String):Void {
		#if neko
		lock.acquire();
		#end
		metrics.get(table).readErrors++;
		#if neko
		lock.release();
		#end
	}
	
	public function writeFailed (table:String):Void {
		#if neko
		lock.acquire();
		#end
		metrics.get(table).writeErrors++;
		#if neko
		lock.release();
		#end
	}
	
	function tick ():Void {
		#if neko
		while (!doShutdown) {
			lock.acquire();
			#end
			
			
			
			#if neko
			lock.release();
			Sys.sleep(5);
		}
		#end
	}
	
}