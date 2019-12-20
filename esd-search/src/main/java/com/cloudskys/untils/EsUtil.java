package com.cloudskys.untils;


import com.alibaba.fastjson.JSON;
import com.cloudskys.domain.UserLocationVo;
import com.cloudskys.service.ElasticSearchService;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Component
public class EsUtil {
    private static final Logger log = LoggerFactory.getLogger(ElasticSearchService.class);
    private static final String INDEX_NAME = "test";
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

    public  Integer addIndexData() throws Exception {
        // Client client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        List<String> cityList = new ArrayList<String>();

        double lat = 39.929986;
        double lon = 116.395645;
        for (int i = 100000; i < 1000000; i++) {
            double max = 0.00001;
            double min = 0.000001;
            Random random = new Random();
            double s = random.nextDouble() % (max - min + 1) + max;
            DecimalFormat df = new DecimalFormat("######0.000000");
            // System.out.println(s);
            String lons = df.format(s + lon);
            String lats = df.format(s + lat);
            Double dlon = Double.valueOf(lons);
            Double dlat = Double.valueOf(lats);

            UserLocationVo city1 = new UserLocationVo(i, "郭德纲"+i, dlat, dlon);
            cityList.add(JSON.toJSONString(city1));
        }

        BulkRequest request = new BulkRequest();
        for (int i = 0;i<cityList.size();i++){
            request.add(new IndexRequest(INDEX_NAME).source(cityList.get(i), XContentType.JSON));
        }

        BulkResponse bulkAddResponse = client.bulk(request, RequestOptions.DEFAULT);
        if (bulkAddResponse.hasFailures()) {
            System.out.println("批量创建索引错误！");
        }
        return null;
    }

    public static void main(String[] args) {
        String roleName=EnumRoleOperation.ROLE_NORMAL_ADMIN.toString();
        String name ="";
        System.out.println(EnumRoleOperation.valueOf(roleName).op(name));
    }
}