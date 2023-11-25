package com.mrbarin.kafka.to.elastic.service.transformer;

import com.mrbarin.elastic.model.index.impl.TwitterIndexModel;
import com.mrbarin.kafka.avro.model.TwitterAvroModel;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class AvroToElasticModelTransformer {
    public List<TwitterIndexModel> getElasticModels(List<TwitterAvroModel> avroModels) {
        return avroModels.stream().map(twitterAvroModel ->
                TwitterIndexModel.builder()
                        .userId(twitterAvroModel.getUserId())
                        .id(String.valueOf(twitterAvroModel.getId()))
                        .text(twitterAvroModel.getText())
                        .createdAt(LocalDateTime.ofInstant(Instant.ofEpochMilli(twitterAvroModel.getCreatedAt()),
                                ZoneId.systemDefault()))
                        .build()
        ).collect(Collectors.toList());
    }
}
