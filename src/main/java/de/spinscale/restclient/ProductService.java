package de.spinscale.restclient;

import java.io.IOException;
import java.util.List;

public interface ProductService {

    Product findById(String id) throws IOException;

    Page<Product> search(String query) throws IOException;

    /**
     * Search the next page of current page, with the same size
     *
     * @param page current page
     * @return the next page
     * @throws IOException
     */
    Page<Product> next(Page<Product> page) throws IOException;

    void save(Product product) throws IOException;

    void save(List<Product> products) throws IOException;
}
