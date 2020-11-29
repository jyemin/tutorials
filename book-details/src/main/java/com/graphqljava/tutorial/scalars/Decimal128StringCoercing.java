package com.graphqljava.tutorial.scalars;

import graphql.language.StringValue;
import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import org.bson.types.Decimal128;

public class Decimal128StringCoercing implements Coercing<Decimal128, String> {
    @Override
    public String serialize(Object dataFetcherResult) throws CoercingSerializeException {
        if (dataFetcherResult instanceof Decimal128) {
            return dataFetcherResult.toString();
        } else {
            throw new CoercingSerializeException();
        }
    }

    @Override
    public Decimal128 parseValue(Object input) throws CoercingParseValueException {
        if (input instanceof String) {
            try {
                return Decimal128.parse((String) input);
            } catch (Exception e) {
                throw new CoercingParseValueException("Could not parse string as Decimal128", e);
            }
        } else {
            throw new CoercingParseValueException("Could not parse " + input.getClass().getSimpleName() + " as Decimal128");
        }
    }

    @Override
    public Decimal128 parseLiteral(Object input) throws CoercingParseLiteralException {
        if (input instanceof StringValue) {
            return parseValue(((StringValue) input).getValue());
        } else {
            throw new CoercingParseValueException("Could not parse " + input.getClass().getSimpleName() + " as Decimal128");
        }
    }
}
