/****
* Copyright (C) 2013 Sam MacPherson
* 
* Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
* 
* The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
****/



package com.amazonaws.elasticmapreduce;
import com.amazonaws.auth.Sig2Http;
import haxe.io.BytesOutput;

enum StepFailureAction {
	TERMINATE;
	CANCEL;
	CONTINUE;
}

typedef Step = {
	name:String,
	jar:String,
	?args:Array<String>,
	?actionOnFailure:StepFailureAction
};

enum InstanceGroupMarket {
	ONDEMAND;
	SPOT;
}

enum InstanceType {
	M1_SMALL;
	M1_MEDIUM;
	C1_MEDIUM;
	M1_LARGE;
	M1_XLARGE;
	C1_XLARGE;
	M2_XLARGE;
	M2_2XLARGE;
	M2_4XLARGE;
	CC2_8XLARGE;
}

typedef InstanceGroup = {
	?type:InstanceType,
	?count:Int,
	?market:InstanceGroupMarket,
	?bidPriceUSD:Float
};

/**
 * Primary class for executing EMR jobs.
 * 
 * @author Sam MacPherson
 */

class ElasticMapReduce {
	
	static inline var API_VERSION:String = "2009-03-31";
	
	static inline var ACTION_ADD_INSTANCE_GROUPS:String = "AddInstanceGroups";
	static inline var ACTION_ADD_JOB_FLOW_STEPS:String = "AddJobFlowSteps";
	static inline var ACTION_DESCRIBE_JOB_FLOWS:String = "DescribeJobFlows";
	static inline var ACTION_MODIFY_INSTANCE_GROUPS:String = "ModifyInstanceGroups";
	static inline var ACTION_RUN_JOB_FLOW:String = "RunJobFlow";
	static inline var ACTION_SET_TERMINATION_PROTECTION:String = "SetTerminationProtection";
	static inline var ACTION_SET_VISIBLE_TO_ALL_USERS:String = "SetVisibleToAllUsers";
	static inline var ACTION_TERMINATE_JOB_FLOWS:String = "TerminateJobFlows";
	
	var config:EMRConfig;
	
	public function new (config:EMRConfig) {
		this.config = config;
	}
	
	function mapInstanceType (type:InstanceType):String {
		if (type == null) type = M1_SMALL;
		
		return switch (type) {
			case M1_SMALL: "m1.small";
			case M1_MEDIUM: "m1.medium";
			case C1_MEDIUM: "c1.medium";
			case M1_LARGE: "m1.large";
			case M1_XLARGE: "m1.xlarge";
			case C1_XLARGE: "c1.xlarge";
			case M2_XLARGE: "m2.xlarge";
			case M2_2XLARGE: "m2.2xlarge";
			case M2_4XLARGE: "m2.4xlarge";
			case CC2_8XLARGE: "cc2.8xlarge";
		}
	}
	
	function mapMarket (type:InstanceGroupMarket):String {
		return switch (type) {
			case ONDEMAND: "ON_DEMAND";
			case SPOT: "SPOT";
		}
	}
	
	function mapActionOnFailure (type:StepFailureAction):String {
		return switch (type) {
			case TERMINATE: "TERMINATE_JOB_FLOW";
			case CANCEL: "CANCEL_AND_WAIT";
			case CONTINUE: "CONTINUE";
		}
	}

	public function runJobFlow (name:String, steps:Array<Step>, master:InstanceGroup, ?core:InstanceGroup, ?task:InstanceGroup):Void {
		var params = new Hash<String>();
		params.set("Name", name);
		
		//Instance config
		params.set("Instances.InstanceGroups.member.1.InstanceRole", "MASTER");
		params.set("Instances.InstanceGroups.member.1.InstanceCount", "1");
		params.set("Instances.InstanceGroups.member.1.InstanceType", mapInstanceType(master.type));
		if (master.market != null) params.set("Instances.InstanceGroups.member.1.Market", mapMarket(master.market));
		if (master.bidPriceUSD != null) params.set("Instances.InstanceGroups.member.1.BidPrice", Std.string(master.bidPriceUSD));
		if (core != null) {
			params.set("Instances.InstanceGroups.member.2.InstanceRole", "CORE");
			params.set("Instances.InstanceGroups.member.2.InstanceCount", Std.string(core.count != null ? core.count : 1));
			params.set("Instances.InstanceGroups.member.2.InstanceType", mapInstanceType(core.type));
			if (core.market != null) params.set("Instances.InstanceGroups.member.2.Market", mapMarket(core.market));
			if (core.bidPriceUSD != null) params.set("Instances.InstanceGroups.member.2.BidPrice", Std.string(core.bidPriceUSD));
		}
		if (task != null) {
			params.set("Instances.InstanceGroups.member.3.InstanceRole", "TASK");
			params.set("Instances.InstanceGroups.member.3.InstanceCount", Std.string(task.count != null ? task.count : 1));
			params.set("Instances.InstanceGroups.member.3.InstanceType", mapInstanceType(task.type));
			if (task.market != null) params.set("Instances.InstanceGroups.member.3.Market", mapMarket(task.market));
			if (task.bidPriceUSD != null) params.set("Instances.InstanceGroups.member.3.BidPrice", Std.string(task.bidPriceUSD));
		}
		
		//Step config
		var index = 1;
		for (i in steps) {
			params.set("Steps.member." + index + ".Name", i.name);
			params.set("Steps.member." + index + ".HadoopJarStep.Jar", i.jar);
			if (i.args != null) {
				var index2 = 1;
				for (o in i.args) {
					params.set("Steps.member." + index + ".HadoopJarStep.Args.member." + index2, o);
					
					index2++;
				}
			}
			if (i.actionOnFailure != null) params.set("Steps.member." + index + ".ActionOnFailure", mapActionOnFailure(i.actionOnFailure));
			
			index++;
		}
		
		sendRequest(ACTION_RUN_JOB_FLOW, params);
	}
	
	function sendRequest (operation:String, params:Hash<String>):Dynamic {
		var conn = new Sig2Http((config.ssl ? "https" : "http") + "://" + config.host + "/", config);
		
		conn.setParameter("Action", operation);
		conn.setParameter("Version", API_VERSION);
		for (i in params.keys()) {
			conn.setParameter(i, params.get(i));
		}
		
		var err = null;
		conn.onError = function (msg:String):Void {
			err = msg;
		}
		
		var data:BytesOutput = new BytesOutput();
		conn.applySigning(true);
		conn.customRequest(true, data);
		/*var out:Dynamic;
		try {
			out = Json.parse(data.getBytes().toString());
		} catch (e:Dynamic) {
			throw ConnectionInterrupted;
		}*/
		//if (err != null) formatError(Std.parseInt(err.substr(err.indexOf("#") + 1)), out.__type, out.message);
		//return out;
	}
	
}