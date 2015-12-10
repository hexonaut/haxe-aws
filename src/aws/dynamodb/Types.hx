package aws.dynamodb;

typedef SString = String;

typedef SInt = Int;

typedef SFloat = Float;

typedef SBool = Bool;

typedef SBinary = haxe.io.Bytes;

/** Stores just the Date */
typedef SDate = Date;

/** Stores the date and the time */
typedef SDateTime = Date;

/** Stores a timestamp which is nearly gaurunteed to be unique */
typedef STimeStamp = Date;

/** Allow to store an enum value that does not have parameters as a simple int */
typedef SEnum<E:EnumValue> = E;

/** Store data in a set */
typedef SSet<T> = Array<T>;

/** Same as SInt, SFloat counterparts, but object updates apply an ADD instead of SET to change value for atomic updates. */
typedef SDeltaInt = Int;
typedef SDeltaFloat = Float;