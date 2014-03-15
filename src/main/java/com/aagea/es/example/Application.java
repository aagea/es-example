package com.aagea.es.example;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class Application {
    private static final String HOST=  "localhost";

    private static Client client;
    public static void main(String [] args) throws IOException {
        client = new TransportClient()
            .addTransportAddress(new InetSocketTransportAddress(HOST, 9300));
        readCommand();
    }
    private static void readCommand() throws IOException {
        String command="";
        while (!command.equals("e")){
            System.out.println("(A)dd information - (S)earch information - (E)xit");
            command=  readLine();
            if(command.toLowerCase().equals("a")){
                addDocument();
            }else if(command.toLowerCase().equals("s")){
                searchDocument();
            }else if(command.toLowerCase().equals("e")){
                endApplication();
            }
        }
    }

    private static void endApplication() {
        System.out.println("Goodbye!!");
        client.close();
    }

    private static void searchDocument() throws IOException {
        System.out.println("Query:");
        String queryStr=readLine();
        SearchResponse response=client.prepareSearch("example").setTypes("es")
                .setSearchType(SearchType.DEFAULT)
                .setQuery(QueryBuilders.wildcardQuery("name",queryStr))
                .addFields("name","data")
                .execute()
                .actionGet();
        for(SearchHit hit: response.getHits()){
            System.out.println("Name ==>" + hit.field("name").value());
            System.out.println("Data ==>" + hit.field("data").getValue());
        }

    }

    private static void addDocument() throws IOException {
        System.out.println("Name:");
        String name=readLine();
        System.out.println("Data:");
        String data=readLine();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("name", name)
                .field("data", data)
                .endObject();
        String json= builder.string();
        IndexResponse response = client.prepareIndex("example","es")
                .setSource(json)
                .execute()
                .actionGet();
        if(response.isCreated()){
            System.out.println("OK");
        }

    }

    private static String readLine() throws IOException {
        if (System.console() != null) {
            return System.console().readLine();
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                System.in));
        return reader.readLine();
    }
}
