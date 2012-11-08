haxe-aws
========

A Haxe library for communicating with the Amazon AWS (http://www.amazonaws.com) backend. Also included are some service implementations such as DynamoDB.

Usage is fairly straight forward. Here is an example with DynamoDB:

    var config = new com.amazonaws.auth.IAMConfig("dynamodb.us-east-1.amazonaws.com", "MYACCESSKEY", "MYSECRETKEY", "us-east-1", "dynamodb");
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