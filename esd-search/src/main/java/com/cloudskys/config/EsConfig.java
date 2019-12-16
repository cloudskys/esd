package com.cloudskys.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestClientBuilder.RequestConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.util.Assert;

import java.util.ArrayList;


@Configuration
public class EsConfig {
    private static String hosts = "127.0.0.1:9200"; // 集群地址，多个用,隔开
    //private static int port = 9200; // 使用的端口号
    private static String schema = "http"; // 使用的协议
    private static ArrayList<HttpHost> hostList = null;
    private static int connectTimeOut = 1000; // 连接超时时间
    private static int socketTimeOut = 30000; // 连接超时时间
    private static int connectionRequestTimeOut = 500; // 获取连接的超时时间

    private static int maxConnectNum = 100; // 最大连接数
    private static int maxConnectPerRoute = 100; // 最大路由连接数

    private static String[] httpHosts = null;
    @Value("${elasticsearch.host}")
    private String host;


   /* static {
        hostList = new ArrayList<HttpHost>();
        String[] hostStrs = hosts.split(",");
        for (String host : hostStrs) {
            hostList.add(new HttpHost(host, port, schema));
        }
    }*/
    @Bean(destroyMethod = "close")//这个close是调用RestHighLevelClient中的close
    @Scope("singleton")
    public RestHighLevelClient client() {
        Assert.isNull(this.hosts, "无效的es连接");
        if(StringUtils.isEmpty(host)){
            host = hosts;
        }
        String[] hosts = host.split(",");
        HttpHost[] httpHosts = new HttpHost[hosts.length];
        for (int i = 0; i < httpHosts.length; i++) {
            String h = hosts[i];
            httpHosts[i] = new HttpHost(h.split(":")[0], Integer.parseInt(h.split(":")[1]), "http");
        }

        //RestClientBuilder builder = RestClient.builder(hostList.toArray(new HttpHost[0]));
        RestClientBuilder builder = RestClient.builder(httpHosts);
        // 异步httpclient连接延时配置
        builder.setRequestConfigCallback(new RequestConfigCallback()  {
            @Override
            public Builder customizeRequestConfig(Builder requestConfigBuilder) {
                requestConfigBuilder.setConnectTimeout(connectTimeOut);
                requestConfigBuilder.setSocketTimeout(socketTimeOut);
                requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeOut);
                return requestConfigBuilder;
            }
        });
        // 异步httpclient连接数配置
        builder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                httpClientBuilder.setMaxConnTotal(maxConnectNum);
                httpClientBuilder.setMaxConnPerRoute(maxConnectPerRoute);
                return httpClientBuilder;
            }
        });
      /*  builder.setFailureListener(new RestClient.FailureListener(){
            @Override
            public void onFailure(Node node) {
                System.out.println("监听es节点失败"+node.getName());
            }
        });
        builder.setRequestConfigCallback(builder1 ->
                builder1.setConnectTimeout(5000).setSocketTimeout(15000));*/
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

}