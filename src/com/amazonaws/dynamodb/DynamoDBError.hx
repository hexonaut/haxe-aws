package com.amazonaws.dynamodb;

/**
 * Represents an error that the database can throw.
 * See http://docs.amazonwebservices.com/amazondynamodb/2011-12-05/developerguide/ErrorHandling.html
 * 
 * @author Sam MacPherson
 */

enum DynamoDBError {
	AccessDeniedException;
	ConditionalCheckFailedException;
	IncompleteSignatureException;
	LimitExceededException;
	MissingAuthenticationTokenException;
	ResourceInUseException;
	ResourceNotFoundException;
	ValidationException;
	RequestTooLarge;
}