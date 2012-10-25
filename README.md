haxe-aws
========

A Haxe library for communicating with the Amazon AWS (http://www.amazonaws.com) backend. Also included are some service implementations such as DynamoDB.

Usage is fairly straight forward. Here is an example with DynamoDB:

    var config = new com.amazonaws.auth.IAMConfig("dynamodb.us-east-1.amazonaws.com", "MYACCESSKEY", "MYSECRETKEY", "us-east-1", "dynamodb");
    var db = new com.amazonaws.dynamodb.Database(config);
	for (i in db.listTables().tableNames) {
		trace("Found table: " + i);
		
		//Add item to the table
		var item = new Attributes();
		item.set("MYPRIMARYHASHKEY", 0);
		db.putItem(i, item);
		
		//And delete the item
		db.deleteItem(i, { hash:0 });
	}