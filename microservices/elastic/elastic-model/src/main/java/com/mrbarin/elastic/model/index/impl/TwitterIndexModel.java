package com.mrbarin.elastic.model.index.impl;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mrbarin.elastic.model.index.IndexModel;
import lombok.Builder;
import lombok.Data;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.time.LocalDateTime;

@Data
@Builder
@Document(indexName = "#{elasticConfigData.indexName}") //indicates this class is a candidate for mapping to elasticsearch
public class TwitterIndexModel implements IndexModel {

    @JsonProperty
    private String id;

    @JsonProperty
    private Long userId;

    @JsonProperty
    private String text;

    @Field(type = FieldType.Date, format = DateFormat.custom, pattern = "uuuu-MMM-dd'T'HH:mm:ssZZ")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "uuuu-MMM-dd'T'HH:mm:ssZZ") // formats the field when converting object to json by using the pattern specified
    @JsonProperty
    private LocalDateTime createdAt;

    @Override
    public String getId() {
        return id;
    }
}
