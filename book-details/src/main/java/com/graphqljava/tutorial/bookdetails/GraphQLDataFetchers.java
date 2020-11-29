package com.graphqljava.tutorial.bookdetails;

import com.graphqljava.tutorial.codecs.OffsetDateTimeCodec;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingFieldSelectionSet;
import graphql.schema.SelectedField;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Aggregates.match;
import static java.util.Arrays.asList;

@Component
public class GraphQLDataFetchers {

    private final MongoCollection<Document> books;

    public GraphQLDataFetchers(@Autowired MongoClient client) {
        MongoDatabase booksDatabase = client.getDatabase("books");
        this.books = booksDatabase.getCollection("books")
                .withCodecRegistry(
                        CodecRegistries.fromRegistries(
                                CodecRegistries.fromCodecs(new OffsetDateTimeCodec()),
                                CodecRegistries.fromProviders(
                                        new DocumentCodecProvider(
                                                new BsonTypeClassMap(Map.of(BsonType.DATE_TIME, OffsetDateTime.class)))),
                                booksDatabase.getCodecRegistry()));
    }

    @PostConstruct
    public void init() {
        books.drop();
        books.insertMany(asList(
                new Document(Map.of(
                        "_id", new ObjectId("5fc0439c8a074765114ef626"),
                        "name", "Harry Potter and the Philosopher's Stone",
                        "price", Decimal128.parse("10.99"),
                        "publicationDate", OffsetDateTime.parse("1997-06-26T00:00:00Z"),
                        "pageCount", 223,
                        "authors", asList(Map.of(
                                "_id", "author-1",
                                "firstName", "Joanne",
                                "lastName", "Rowling")))),
                new Document(Map.of(
                        "_id", new ObjectId("5fc0439c8a074765114ef627"),
                        "name", "Moby Dick",
                        "price", Decimal128.parse("3.99"),
                        "publicationDate", OffsetDateTime.parse("1851-10-18T00:00:00Z"),
                        "pageCount", 635,
                        "authors", asList(Map.of(
                                "_id", "author-2",
                                "firstName", "Herman",
                                "lastName", "Melville")))),
                new Document(Map.of(
                        "_id", new ObjectId("5fc0439c8a074765114ef628"),
                        "name", "Interview with the Vampire",
                        "price", Decimal128.parse("9.99"),
                        "publicationDate", OffsetDateTime.parse("1976-05-05T00:00:00Z"),
                        "pageCount", 371,
                        "authors", asList(Map.of(
                                "firstName", "Anne",
                                "lastName", "Rice"))))));
    }


    public DataFetcher<List<Map<String, Object>>> getBooksDataFetcher() {
        return dataFetchingEnvironment -> books.aggregate(asList(
                createProjectStage(dataFetchingEnvironment.getSelectionSet())))
                .into(new ArrayList<>());
    }

    public DataFetcher<Map<String, Object>> getBookByIdDataFetcher() {
        return dataFetchingEnvironment -> books.aggregate(asList(
                match(Filters.eq(new ObjectId(dataFetchingEnvironment.<String>getArgument("id")))),
                createProjectStage(dataFetchingEnvironment.getSelectionSet())))
                .first();
    }

    public DataFetcher<List<Map<String, Object>>> getBooksByPriceDataFetcher() {
        return dataFetchingEnvironment ->
                books.aggregate(asList(
                        match(Filters.lt("price", dataFetchingEnvironment.<Decimal128>getArgument("lessThan"))),
                        createProjectStage(dataFetchingEnvironment.getSelectionSet())))
                        .into(new ArrayList<>());
    }

    private Bson createProjectStage(DataFetchingFieldSelectionSet selectionSet) {
        return new Document("$project",
                project(new Document("_id", 0), selectionSet.getImmediateFields(), ""));
    }

    private Bson project(Document projection, List<SelectedField> fields, String root) {
        fields.forEach(field -> {
            String key = root.equals("") ? field.getName() : (root + "." + field.getName());
            if (field.getSelectionSet().getImmediateFields().isEmpty()) {
                if (key.equals("id")) {
                    projection.put("id", new Document("$toString", "$_id"));
                } else {
                    projection.put(key, 1);
                }
            } else {
                project(projection, field.getSelectionSet().getImmediateFields(), key);
            }
        });
        return projection;
    }
}
