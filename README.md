haxe-aws
========

A Haxe library for communicating with the Amazon AWS (http://www.amazonaws.com) backend. haxe-aws is cross-platform at its core, but the implementations are only designed for sys platforms.

Implemented services:

*	IAM Authentication (Signature V2 and V4)
*	DynamoDB
*	Elastic MapReduce

DynamoDB
========

Since v0.2.0 the DynamoDB implementation has been built to mirror the Haxe SPOD API. Here is an example object taken from the SPOD documentation page (http://haxe.org/manual/spod) reworked to DynamoDB:

	import aws.dynamodb.Manager;
	import aws.dynamodb.Object;
	import aws.dynamodb.Types;

	@:table("user")
	@:id(id)
	class User extends Object {
		
		public var id:SInt;
		public var name:SString;
		public var birthday:SDate;
		public var phoneNumber:SString;
		
		public static var manager = new UserManager();

		public function new () {
			super();
		}
		
	}

	class UserManager extends Manager<User> {
		
		public function new () {
			super(User);
		}
		
	}

As you can see there are very few differences. If you are familiar with Haxe SPOD then you will adjust quickly to using DynamoDB SPOD. I'll go over some of the differences.

Types
-----

Types are not as specific in DynamoDB SPOD. For example Haxe SPOD has SSmallText, SMediumText, SString<Const>, etc whereas DynamoDB SPOD just has SString. All of the types that use the same name in both directly translate over.

Some types that are special to DynamoDB:

 * SSet<T> - Stores a list of items. Maps directly to the "Set" DynamoDB type.
 * STimeStamp - It is often useful to produce a unique timestamp in DynamoDB for temporally ordered, but unique entries. STimeStamp does just that by filling the DateTime to maximum precision with random digits. This way you can query entries in temporal order (within a second accuracy) while still maintaining required uniqueness.

Meta Data
---------

 * @:prefix("TABLE_PREFIX") - All tables in DynamoDB share the same namespace so it is often nice to prefix them to emulate a seperate Database. Alternatively you can set the aws.dynamodb.Manager.prefix to set the prefix globally.
 * @:table("TABLE_NAME") - The name of the table. Required.
 * @:shard("_%Y-%m-%d") - Often you want to rotate tables on a regular basis to minimize DynamoDB storage costs. The shard meta data will be appended to the table name after being sent through the DateTools.format() function performed on the current time.
 * @:id(hash, range) - Specify the primary index of the table. The range identifier is optional. The id meta data is required.
 * @:read(readCapacity) - Specify the read capacity of this table. Defaults to 1 if not specified.
 * @:write(writeCapacity) - Specify the write capacity of this table. Defaults to 1 if not specified.
 * @:lindex("IndexName", hash, secondary_range) - Specify a local secondary index. Range is optional.
 * @:gindex("IndexName", secondary_hash, secondary_range, readCapacity, writeCapacity) - Specify a global secondary index. Range read and write capacity are optional.

Search
------

	import aws.dynamodb.Manager;
	import aws.dynamodb.Object;
	import aws.dynamodb.Types;

	@:table("post")
	@:id(thread, postdate)
	@:gindex("UserIndex", poster, postdate)
	class Post extends Object {
		
		public var thread:SString;
		public var poster:SString;
		public var message:SString;
		public var postdate:STimeStamp;
		
		public static var manager = new PostManager();

		public function new () {
			super();
		}
		
	}

	class PostManager extends Manager<Post> {
		
		public function new () {
			super(Post);
		}
		
	}
	
	//List posts in this thread
	for (i in Post.manager.search($thread == "A Forum Thread", { orderBy:postdate, limit:10 })) {
		trace(i.message);
	}
	
	//List most recent posts by this user that occurred on or before Jan 1st, 2014
	for (i in Post.manager.search($poster == "User123" && $postdate <= new Date(2014, 0, 1, 0, 0, 0), { orderBy:-postdate, limit:10 })) {
		trace(i.message);
	}

Search is built to look as close to regular Haxe SPOD search as possible. The only difference is the flexibility of the queries. DynamoDB only allows queries on a hash key followed by simple comparisons to the range key. Really all your queries should be structured like this:

	$hash == "Some Hash" && $range OP OPERAND

NOTE: The orderBy clause must always be the range key of the comparison.

NOTE: The index of the search will be automatically inferred based on the arguments of the search/select conditional.

Allowed Range Operators:

==, >, >=, <, <=

Relations
---------

DynamoDB SPOD is built to fully interoperate with Haxe SPOD. Relations can be made between objects residing on different databases using the usual Haxe SPOD syntax.

Elastic MapReduce
=================

Here is how you run a custom script with Amazon EMR:

	var config = new com.amazonaws.elasticmapreduce.EMRConfig("elasticmapreduce.us-east-1.amazonaws.com", "MYACCESSKEY", "MYSECRETKEY", "us-east-1");
    var emr = new com.amazonaws.elasticmapreduce.ElasticMapReduce(config);
	
	emr.runJobFlow("TestJob", 
		[{name:"Step1", jar:"s3://elasticmapreduce/libs/script-runner/script-runner.jar", args:["s3://mybucket/path/to/script"]}],
		{ type:M1_SMALL }
	);
