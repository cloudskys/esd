package com.cloudskys.untils;


import com.cloudskys.service.ElasticSearchService;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class EsUtil {
    private static final Logger log = LoggerFactory.getLogger(ElasticSearchService.class);

    @Autowired
    private RestHighLevelClient client;

    /**
     * 判断索引是否存在
     *
     * @param indexName
     * @return
     */
    public boolean isIndexExists(String indexName) {
        boolean exists = false;
        try {
            GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
            getIndexRequest.humanReadable(true);
            exists = client.indices().exists(getIndexRequest,RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return exists;
    }

    /**
     * 删除索引
     *
     * @param indexName
     * @return
     */
    public boolean deleteIndex(String indexName) {
        boolean acknowledged = false;
        try {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
           // deleteIndexRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
            AcknowledgedResponse delete = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            acknowledged = delete.isAcknowledged();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return acknowledged;
    }
}