package com.cloudskys.controller;
import com.cloudskys.service.ElasticSearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController 
@RequestMapping("/test")
public class TestResource {
    @Autowired
    private ElasticSearchService elasticSearchService;
    private static final Logger log = LoggerFactory.getLogger(TestResource.class);
    //条件 查询 
    @PostMapping("/v1/test")
    public String query( ) {
        String queryResult =null;
        try {
            queryResult = elasticSearchService.search("");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return queryResult;
    }
}