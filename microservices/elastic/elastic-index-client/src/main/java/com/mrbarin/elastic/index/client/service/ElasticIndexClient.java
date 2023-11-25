package com.mrbarin.elastic.index.client.service;

import com.mrbarin.elastic.model.index.IndexModel;

import java.util.List;

public interface ElasticIndexClient <T extends IndexModel> {
    List<String> save(List<T> documents);
}
