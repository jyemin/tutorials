package com.graphqljava.tutorial.bookdetails;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingFieldSelectionSet;
import graphql.schema.SelectedField;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Aggregates.match;
import static java.util.Arrays.asList;

@Component
public class GraphQLDataFetchers {

    private final MongoCollection<Document> books;

    public GraphQLDataFetchers(@Autowired MongoClient client) {
        books = client.getDatabase("books").getCollection("books");
    }

    @PostConstruct
    public void init() {
        books.drop();
        books.insertMany(asList(
                new Document(Map.of(
                        "_id", new ObjectId("5fc0439c8a074765114ef626"),
                        "name", "Harry Potter and the Philosopher's Stone",
                        "pageCount", 223,
                        "authors", asList(Map.of(
                                "_id", "author-1",
                                "firstName", "Joanne",
                                "lastName", "Rowling")))),
                new Document(Map.of(
                        "_id", new ObjectId("5fc0439c8a074765114ef627"),
                        "name", "Moby Dick",
                        "pageCount", 635,
                        "authors", asList(Map.of(
                                "_id", "author-2",
                                "firstName", "Herman",
                                "lastName", "Melville")))),
                new Document(Map.of(
                        "_id", new ObjectId("5fc0439c8a074765114ef628"),
                        "name", "Interview with the Vampire",
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
