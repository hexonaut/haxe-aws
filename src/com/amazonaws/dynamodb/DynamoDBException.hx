package com.amazonaws.dynamodb;

/**
 * Represents an error that the database can throw. Similar to DynamoDBError, but these requests should be retried.
 * See http://docs.amazonwebservices.com/amazondynamodb/2011-12-05/developerguide/ErrorHandling.html
 * 
 * @author Sam MacPherson
 */

enum DynamoDBException {
	ProvisionedThroughputExceededException;
	ThrottlingException;
	InternalFailure;
	InternalServerError;
	ServiceUnavailableException;
}