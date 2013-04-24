haxe-aws
========

A Haxe library for communicating with the Amazon AWS (http://www.amazonaws.com) backend. haxe-aws has only been tested on neko, but it is designed to be fully cross platform.

Implemented services:

*	IAM Authentication (Signature V2 and V4)
*	DynamoDB (Except batch read/write)
*	Elastic MapReduce

DynamoDB Examples
-----------------

Using DynamoDB is fairly straight forward.

    var config = new com.amazonaws.dynamodb.DynamoDBConfig("dynamodb.us-east-1.amazonaws.com", "MYACCESSKEY", "MYSECRETKEY", "us-east-1");
    var db = new com.amazonaws.dynamodb.Database(config);
	
	//Add 3 items to myTable
	db.putItem("myTable", {id:0, rangeid:0 someVar:"Haxe Rocks!"});
	db.putItem("myTable", {id:0, rangeid:1, someVar:"Haxe really Rocks!"});
	db.putItem("myTable", {id:1, rangeid:0, someBinaryVar:haxe.io.Bytes.ofString("DynamoDB supports binary data too!")});
	
	//Print the second items 'someVar' attribute
	trace(db.getItem("myTable", {id:0, rangeid:1}).someVar);	//Will print "Haxe really Rocks!"
	
	//Count the number of items in myTable
	trace(Collection.scan(db, "myTable").count());		//Will print "3"
	
	//Scan myTable
	for (i in Collection.scan(db, "myTable")) {
		trace(i);
	}
	
	//Query myTable for items with hash key 0
	for (i in Collection.query(db, "myTable", 0)) {
		trace(i);
	}
	
	//Query myTable for items with hash key 0 but limit to the first result
	for (i in Collection.query(db, "myTable", 0, {limit:1})) {
		trace(i);
	}

You can also extend PersistantObject on your custom object to simplify usage. Below is an example of storing a Customer object with DynamoDB:

	@table("customer")	//Required
	@hash("id")			//Required
	@range("range")		//Required if the table is using a range
	class Customer extends com.amazonaws.dynamodb.PersistantObject {
		
		public var id:Int;
		public var range:String;
		public var theTime:Date;
		@ignore public var extraData:String;	//This will not be inserted into the database
		
		public function new () {
			super();
		}
		
	}
	
	var config = new com.amazonaws.dynamodb.DynamoDBConfig("dynamodb.us-east-1.amazonaws.com", "MYACCESSKEY", "MYSECRETKEY", "us-east-1");
    var db = new com.amazonaws.dynamodb.Database(config);
	
	PersistantObject.DATABASE = db;
	PersistantObject.TABLE_PREFIX = "table_prefix_";
	
	//Insert a new object into the database
	var c = new Customer();
	c.id = 0;				//Required
	c.range = "testing";	//Required
	c.theTime = Date.now();
	c.extraData = "this string not be inserted into the database";
	c.insert();
	
	//Get some object
	var c2 = new Customer();
	c2.id = 0;
	c2.range = "testing";
	c2.get();
	trace(c2.theTime);		//Returns the current time
	trace(c2.extraData);	//Returns null
	
	//Update an attribute
	c2.theTime = Date.now();
	c2.update();
	
	//Delete the object
	c2.delete();
	
Supported types are Bool, Int, Float, String, Date and Bytes.

Elastic MapReduce Examples
--------------------------

Here is how you run a custom script with Amazon EMR:

	var config = new com.amazonaws.elasticmapreduce.EMRConfig("elasticmapreduce.us-east-1.amazonaws.com", "MYACCESSKEY", "MYSECRETKEY", "us-east-1");
    var emr = new com.amazonaws.elasticmapreduce.ElasticMapReduce(config);
	
	emr.runJobFlow("TestJob", 
		[{name:"Step1", jar:"s3://elasticmapreduce/libs/script-runner/script-runner.jar", args:["s3://mybucket/path/to/script"]}],
		{ type:M1_SMALL }
	);
