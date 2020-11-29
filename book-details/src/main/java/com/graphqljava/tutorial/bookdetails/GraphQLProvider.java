package com.graphqljava.tutorial.bookdetails;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.graphqljava.tutorial.scalars.Decimal128StringCoercing;
import graphql.GraphQL;
import graphql.scalars.ExtendedScalars;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.URL;

import static graphql.schema.idl.TypeRuntimeWiring.newTypeWiring;

@Component
public class GraphQLProvider {
    private final GraphQLDataFetchers graphQLDataFetchers;
    private GraphQL graphQL;

    public GraphQLProvider(@Autowired GraphQLDataFetchers graphQLDataFetchers) {
        this.graphQLDataFetchers = graphQLDataFetchers;
    }

    @SuppressWarnings("UnstableApiUsage")
    @PostConstruct
    public void init() throws IOException {
        URL url = Resources.getResource("schema.graphqls");
        String sdl = Resources.toString(url, Charsets.UTF_8);
        GraphQLSchema graphQLSchema = buildSchema(sdl);
        this.graphQL = GraphQL.newGraphQL(graphQLSchema).build();
    }

    private GraphQLSchema buildSchema(String sdl) {
        TypeDefinitionRegistry typeRegistry = new SchemaParser().parse(sdl);
        RuntimeWiring runtimeWiring = buildWiring();
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        return schemaGenerator.makeExecutableSchema(typeRegistry, runtimeWiring);
    }

    private RuntimeWiring buildWiring() {
        return RuntimeWiring.newRuntimeWiring()
                .scalar(ExtendedScalars.DateTime)
                .scalar(GraphQLScalarType.newScalar()
                        .name("Decimal128")
                        .description("An IEEE 754-2008 binary integer decimal representation of a 128-bit decimal value, " +
                                "supporting 34 decimal digits of significand and an exponent range * of -6143 to +6144")
                        .coercing(new Decimal128StringCoercing())
                        .build())
                .type(newTypeWiring("Query")
                        .dataFetcher("books", graphQLDataFetchers.getBooksDataFetcher()))
                .type(newTypeWiring("Query")
                        .dataFetcher("bookById", graphQLDataFetchers.getBookByIdDataFetcher()))
                .type(newTypeWiring("Query")
                        .dataFetcher("booksByPrice", graphQLDataFetchers.getBooksByPriceDataFetcher()))
                .build();
    }

    @Bean
    public GraphQL graphQL() {
        return graphQL;
    }

}
