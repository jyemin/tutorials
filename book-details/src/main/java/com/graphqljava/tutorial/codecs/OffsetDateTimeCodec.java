package com.graphqljava.tutorial.codecs;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class OffsetDateTimeCodec implements Codec<OffsetDateTime> {
    @Override
    public OffsetDateTime decode(BsonReader reader, DecoderContext decoderContext) {
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(reader.readDateTime()), ZoneOffset.UTC);
    }

    @Override
    public void encode(BsonWriter writer, OffsetDateTime value, EncoderContext encoderContext) {
        writer.writeDateTime(value.toInstant().toEpochMilli());
    }

    @Override
    public Class<OffsetDateTime> getEncoderClass() {
        return OffsetDateTime.class;
    }
}
