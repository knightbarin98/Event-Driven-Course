package com.mrbarin.elastic.index.client.util;

import com.mrbarin.elastic.model.index.IndexModel;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class ElasticIndexUtil<T extends IndexModel> {

    /**
     * Stream twitter index model documents to send to elastic search
     * as queries
     *
     * @param documents
     * @return
     */
    public List<IndexQuery> getIndexQueries(List<T> documents) {
        return documents.stream()
                .map(document ->
                        new IndexQueryBuilder()
                                .withIndex(document.getId())
                                .withObject(document)
                                .build()
                ).collect(Collectors.toList());
    }
}
