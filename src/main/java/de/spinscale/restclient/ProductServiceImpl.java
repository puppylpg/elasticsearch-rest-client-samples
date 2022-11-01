package de.spinscale.restclient;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

// use async in a real application
public class ProductServiceImpl implements ProductService {

    private final String index;
    private final ElasticsearchClient client;

    public ProductServiceImpl(String index, ElasticsearchClient client) {
        this.index = index;
        this.client = client;
    }

    @Override
    public Product findById(String id) throws IOException {
        final GetResponse<Product> getResponse = client.get(builder -> builder.index(index).id(id), Product.class);
        return getResponse.source();
    }

    @Override
    public Page<Product> search(String input) throws IOException {
        return getPageResult(createSearchRequest(input, 0, 10), input);
    }

    @Override
    public Page<Product> next(Page<Product> page) throws IOException {
        int from = page.getFrom() + page.getSize();
        final SearchRequest request = createSearchRequest(page.getInput(), from, page.getSize());
        return getPageResult(request, page.getInput());
    }

    private Page<Product> getPageResult(SearchRequest searchRequest, String input) throws IOException {
        final SearchResponse<Product> response = client.search(searchRequest, Product.class);
        if (response.hits().total().value() == 0) {
            return Page.empty();
        }
        if (response.hits().hits().isEmpty()) {
            return Page.empty();
        }

        final List<Product> products = response.hits().hits().stream().map(Hit::source).collect(Collectors.toList());
        return new Page<>(products, input, searchRequest.from(), searchRequest.size());
    }

    private SearchRequest createSearchRequest(String input, int from, int size) {
        String comment = """
                GET /_search
                {
                  "query": {
                    "multi_match" : {
                      "query":    "this is a test",
                      "fields": [ "name", "description" ]
                    }
                  }
                }
                """;
        return new SearchRequest.Builder()
                .from(from)
                .size(size)
                .query(
                        qb -> qb.multiMatch(
                                mmqb -> mmqb.query(input).fields("name", "description")
                        )
                )
                .build();
    }

    @Override
    public void save(Product product) throws IOException {
        save(Collections.singletonList(product));
    }

    @Override
    public void save(List<Product> products) throws IOException {
        String comment = """
                POST <index>/_bulk
                {"index":{"_id":"0"}}
                {"id":"0","name":"Name of 0 product","description":"Description of 0 product","price":0.0,"stock_available":0}
                {"index":{"_id":"1"}}
                {"id":"1","name":"Name of 1 product","description":"Description of 1 product","price":1.2,"stock_available":10}
                """;
        final BulkResponse response = client.bulk(builder -> {
            for (Product product : products) {
                builder.index(index)
                       .operations(ob -> {
                           if (product.getId() != null) {
                               ob.index(ib -> ib.id(product.getId()).document(product));
                           } else {
                               ob.index(ib -> ib.document(product));
                           }
                           return ob;
                       });
            }
            return builder;
        });

        final int size = products.size();
        for (int i = 0; i < size; i++) {
            products.get(i).setId(response.items().get(i).id());
        }
    }
}
