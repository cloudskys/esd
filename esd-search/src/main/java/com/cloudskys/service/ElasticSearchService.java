package com.cloudskys.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cloudskys.domain.NID;
import com.cloudskys.domain.ResponseBean;
import com.cloudskys.domain.User;
import com.cloudskys.untils.EsContants;
import com.cloudskys.untils.EsUtil;
import com.cloudskys.untils.JsonUtil;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.ParsedValueCount;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * QueryBuilders.termQuery("key", obj) 完全匹配
 * QueryBuilders.termsQuery("key", obj1, obj2..)   一次匹配多个值 terms 用法，类似于数据库的 in
 * QueryBuilders.matchQuery("key", Obj) 单个匹配, field不支持通配符, 前缀具高级特性
 * QueryBuilders.multiMatchQuery("text", "field1", "field2"..);  匹配多个字段, field有通配符忒行
 * QueryBuilders.matchAllQuery();         匹配所有文件
 * Bool Query 用于组合多个叶子或复合查询子句的默认查询
 * must 相当于 与 & =
 * must not 相当于 非 ~ ！=
 * should 相当于 或 | or
 * filter 过滤
 */


/**
 * {
 *   "query": {
 *     "bool": {
 *       "must": [
 *         { "range": { "@timestamp": { "gt": "2018-02-08T07:00:00.056000000+00:00","lt": "2018-02-08T08:00:00.056000000+00:00" }  }  }
 *         ,
 *         {  "wildcard": { "message": "*cp_geo*"  }  }
 *         ,
 *         { "match": { "message": "*type:platform*" } }
 *       ],
 *       "must_not": { "match": { "message": "*deviceTypeCode:DTout00000000*" } },
 *       "should": []
 *     }
 *   },
 *   "from": 0,
 *   "size": 50,
 *   "sort": {  "@timestamp": "desc"  },
 *   "aggs": {}
 * }
 *
 * 1. 查询出包含 log_geo 的数据 “wildcard”: { “message”: “log_geo” }
 * 此处 log_geo 前面有*表示搜索以log_geo结尾的数据
 * log_geo后面有* 表示搜以log_geo开始的数据，
 * log_geo前后都有*就是通用匹配包含log_geo的记录
 * “wildcard”: { “message”: “log_geo” }
 * 2. 查询某个时间段的数据
 * “range”: { “@timestamp”: { “gt”: “2018-02-08T07:00:00.056000000+00:00”,”lt”: “2018-02-08T08:00:00.056000000+00:00” } }
 * #注意 时区减去8小时
 * 3. 条件查询与条件排除数据
 * 3.1 match 包含provider 的数据
 * { “match”: { “message”: “type:provider” } }
 * 3.2 must_not 类似于 must 做排除使用
 * 排除包含 “must_not”: { “match”: { “message”: “dateTime:2018-02-08 15:59” } },
 * 4. from 表示起始的记录的ID
 * 5. size 表示显示的记录数
 * ————————————————
 *
 */
@Service
public class ElasticSearchService {
    private static final Logger log = LoggerFactory.getLogger(ElasticSearchService.class);
    @Autowired
    private RestHighLevelClient client;

    @Autowired
    private EsUtil esUtil;


    public ResponseBean  createIndex2(String indexName){
        createIndex(indexName);
        return null;
    }


    /**
     * 创建索引
     * @param indexName
     */
    public ResponseBean  createIndex(String indexName){
        //CreateIndexRequest 实例， 需要注意包的版本 我这里用的7.2的版本 org.elasticsearch.client.indices;
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        //封装属性 类似于json格式
        Map<String, Object> jsonMap = new HashMap<>();
        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> content = new HashMap<>();
        content.put("type", "integer");
        Map<String, Object> account = new HashMap<>();
        content .put("type", "text");
        content .put("analyzer", "ik_max_word");
        properties.put("id", content);
        properties.put("account", account);
        jsonMap.put("properties", properties);
        //设置分片
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
        );
        request.mapping(jsonMap);

        //为索引设置一个别名 可选
        request.alias(new Alias("twitter_alias"));

        //我使用的同步的方式 异步请参考官方文档
        CreateIndexResponse createIndexResponse = null;

        try {
            createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            boolean acknowledged = createIndexResponse.isAcknowledged();
            if (acknowledged) {
                return new ResponseBean(200, "删除成功", null);
            } else {
                return new ResponseBean(10002, "删除失败", null);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }






//////########################################################################################################################




    /**
     * 创建索引
     * @param indexName
     * @param settings
     * @param mapping
     * @return
     * @throws IOException
     */
    public CreateIndexResponse createIndex(String indexName, String settings, String mapping) throws IOException{
        CreateIndexRequest request = new CreateIndexRequest(indexName);

        if (null != settings && !"".equals(settings)) {
            request.settings(settings, XContentType.JSON);
        }
        if (null != mapping && !"".equals(mapping)) {
            request.mapping(mapping, XContentType.JSON);
        }
        return client.indices().create(request, RequestOptions.DEFAULT);
    }

    /**
     * 删除索引
     * @param indexName
     */
    public void delIndex(String indexName){
        try {
            if (existsIndex(indexName)) {
                boolean isDelete = esUtil.deleteIndex(indexName);
            }
        }catch(Exception e){

        }
    }
    /**
     * 判断 index 是否存在
     * @param indexName
     * @return
     * @throws IOException
     */
    public boolean existsIndex(String indexName) throws IOException {
        GetIndexRequest request = new GetIndexRequest(indexName);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }
    /**
     * 保存数据 手动指定id
     * @param user
     * @return
     * @throws Exception
     */
    public ResponseBean  addDoc(User user) throws Exception {
        if (!existsIndex(EsContants.INDEX_NAME)) {
            createIndex(EsContants.INDEX_NAME);
        }

        IndexRequest request = new IndexRequest(EsContants.INDEX_NAME);
        request.id(user.getId());    //ID也可使用内部自动生成的 不过希望和数据库统一唯一业务ID
        request.source(JSON.toJSONString(user), XContentType.JSON);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        if (indexResponse != null) {
            String id = indexResponse.getId();
            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                return new ResponseBean(200, "插入成功", id);
            } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                System.out.println("修改文档成功!");
                return new ResponseBean(10001, "插入失败", null);
            }
        }
        return null;
    }
    /**
     * 添加文档 使用自动id
     * @param indexName
     * @param source
     */
    public IndexResponse addDoc(String indexName, String source) throws IOException{
        IndexRequest request = new IndexRequest(indexName);
        request.source(source, XContentType.JSON);
        return client.index(request, RequestOptions.DEFAULT);
    }

    /**
     * 判断记录是都存在
     * @param index
     * @param type
     * @param userVo
     * @return
     * @throws IOException
     */
    public boolean exists(String index, String type, User userVo) throws IOException {
        GetRequest getRequest = new GetRequest(index, userVo.getId().toString());
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println("exists: " + exists);
        return exists;
    }

    //查询单个文档
    public Object getDoc(String id) {
        GetRequest getRequest = new GetRequest(EsContants.INDEX_NAME, id);
       try {
           GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
           if (getResponse.isExists()) {
               String sourceAsString = getResponse.getSourceAsString();
               return sourceAsString;
           }
       }catch (Exception e){

       }
        return null;
    }

    //查询单个文档
    public List<User> getAllDoc() {
        List<User> users=null;
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
            searchSourceBuilder.query(queryBuilder);
            SearchRequest searchRequest = new SearchRequest(EsContants.INDEX_NAME).source(searchSourceBuilder);
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = response.getHits().getHits();
            users = new ArrayList<>();
            for (SearchHit user : hits) {
                User userVo = JsonUtil.getObject(user.getSourceAsString(), User.class);
                users.add(userVo);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return users;
    }

    /**
     * 根据 id 更新指定索引中的文档
     * @param indexName
     * @param id
     * @param updateMap
     * @return
     * @throws IOException
     */
    public UpdateResponse updateDoc(String indexName, String id, Map<String,Object> updateMap) throws IOException{
        UpdateRequest request = new UpdateRequest(indexName, id);
        request.doc(updateMap);
        return client.update(request, RequestOptions.DEFAULT);
    }
    public UpdateResponse updateDoc(String indexName, String id, User userVo) throws IOException{
        UpdateRequest request = new UpdateRequest(indexName, id);
        request.doc(JSON.toJSONString(userVo), XContentType.JSON);
        UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
        return updateResponse;
    }

    /**
     * 根据 id 删除指定索引中的文档
     * @param
     * @param id
     */
    public void delete(String id) {
        DeleteRequest request = new DeleteRequest(EsContants.INDEX_NAME, id);
        try {
            DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
            if (deleteResponse.status() == RestStatus.OK) {
                log.info("删除成功！id: {}", id);
            }
        }catch (Exception e){

        }
    }


    public String search(String content) throws IOException {
        //创建检索请求
        SearchRequest searchRequest = new SearchRequest(EsContants.INDEX_NAME); //索引


        //创建搜索构建者
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();

        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("fields.entity_name", "java 程序员 书 推荐");//这里可以根据字段进行搜索，must表示符合条件的，相反的mustnot表示不符合条件的
         RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("fields_timestamp"); //新建range条件
         rangeQueryBuilder.gte("2019-03-21T08:24:37.873Z"); //开始时间
         rangeQueryBuilder.lte("2019-03-21T08:24:37.873Z"); //结束时间
         //boolBuilder.must(rangeQueryBuilder);
        //boolBuilder.must(matchQueryBuilder);
        //sourceBuilder.query(boolBuilder); //设置查询，可以是任何类型的QueryBuilder。
        matchQueryBuilder.minimumShouldMatch("50%");//java 程序员 书 推荐，这里就有 4 个词，假如要求 50% 命中其中两个词就返回
        //matchQueryBuilder.operator(Operator.AND); //与match 本身的or意义相反
        String[] includeFields = new String[] {"fields.port","fields.entity_id","fields.message"};
        String[] excludeFields =  new String[] {""};
        if (!CollectionUtils.isEmpty(includeFields) || !CollectionUtils.isEmpty(excludeFields)) {
            sourceBuilder.fetchSource(includeFields, excludeFields); //返回和排除列 第一个是获取字段，第二个是过滤的字段，默认获取全部
        }

        if (!StringUtils.isEmpty(content)){
            //自定义组合查询  boost是设置权重 /查询结果status为4的会被优先展示其次coutent，再次fields_timestamp
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            TermQueryBuilder termQuery = QueryBuilders.termQuery("status", 4).boost(8);
            //wildcard通配符查询 性能较差不建议使用 ?：任意字符
            // WildcardQueryBuilder termQuery1 = QueryBuilders.wildcardQuery("appli_name", "1?3");
            MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("coutent",content)
                    .fuzziness(Fuzziness.AUTO).boost(1); //模糊匹配
            boolQueryBuilder.must(termQuery).must(queryBuilder).must(rangeQueryBuilder).must(matchQueryBuilder);
                    //.must(termQuery1);
            sourceBuilder.query(boolQueryBuilder);
        }
        sourceBuilder.from(0); //设置确定结果要从哪个索引开始搜索的from选项，默认为0
        sourceBuilder.size(100); //设置确定搜素命中返回数的size选项，默认为10
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)); //设置一个可选的超时，控制允许搜索的时间。
        sourceBuilder.sort(new FieldSortBuilder("id").order(SortOrder.ASC)); //根据自己的需求排序
        // 排序
        //FieldSortBuilder fsb = SortBuilders.fieldSort("date");
        //fsb.order(SortOrder.DESC);
        //sourceBuilder.sort(fsb);

        searchRequest.source(sourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();  //SearchHits提供有关所有匹配的全局信息，例如总命中数或最高分数：
        SearchHit[] searchHits = hits.getHits();
        // List<User> list = Arrays.stream(response.getHits().getHits()).map(this::toHig).collect(Collectors.toList());
        for (SearchHit hit : searchHits) {
            log.info("search -> {}",hit.getSourceAsString());
        }
        return Arrays.toString(searchHits);
    }

    /**
     * 根据某字段的 k-v 更新索引中的文档
     * @param fieldName
     * @param value
     * @param indexName
     * @throws IOException
     */
    public void updateByQuery(String fieldName, String value, String ... indexName) throws IOException {
        UpdateByQueryRequest request = new UpdateByQueryRequest(indexName);
        //单次处理文档数量
        request.setBatchSize(100)
                .setQuery(new TermQueryBuilder(fieldName, value))
                .setTimeout(TimeValue.timeValueMinutes(2));
        client.updateByQuery(request, RequestOptions.DEFAULT);
    }
    /**
     * 根据Id更新 doc
     * @param id
     * @param money
     * @return
     */
    public ResponseBean testESUpdate(@RequestParam String id, @RequestParam Double money) {
        UpdateRequest updateRequest = new UpdateRequest(EsContants.INDEX_NAME, id);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("money", money);
        updateRequest.doc(map);
        try {
            UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
            if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                return new ResponseBean(200, "更新成功", null);
            } else {
                return new ResponseBean(10002, "删除失败", null);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return new ResponseBean(1003, "删除异常", null);
        }
    }

    public UpdateResponse updateDoc(String indexName, String id, String updateJson) throws IOException{
        UpdateRequest request = new UpdateRequest(indexName, id);
        request.doc(XContentType.JSON, updateJson);
        return client.update(request, RequestOptions.DEFAULT);
    }
    /**
     * 普通查询
     * @return
     */
   public ResponseBean findES(){
       SearchRequest searchRequest = new SearchRequest(EsContants.INDEX_NAME);
       SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//如果用name直接查询，其实是匹配name分词过后的索引查到的记录(倒排索引)；如果用name.keyword查询则是不分词的查询，正常查询到的记录
       RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("birthday").from("1991-01-01").to("2010-10-10").format("yyyy-MM-dd");//范围查询
//        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name.keyword", name);//精准查询
       PrefixQueryBuilder prefixQueryBuilder = QueryBuilders.prefixQuery("name.keyword", "张");//前缀查询
//        WildcardQueryBuilder wildcardQueryBuilder = QueryBuilders.wildcardQuery("name.keyword", "*三");//通配符查询
//        FuzzyQueryBuilder fuzzyQueryBuilder = QueryBuilders.fuzzyQuery("name", "三");//模糊查询
       FieldSortBuilder fieldSortBuilder = SortBuilders.fieldSort("age");//按照年龄排序
       fieldSortBuilder.sortMode(SortMode.MIN);//从小到大排序

       BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
       boolQueryBuilder.must(rangeQueryBuilder).should(prefixQueryBuilder);//and or  查询

       sourceBuilder.query(boolQueryBuilder).sort(fieldSortBuilder);//多条件查询
       sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
       searchRequest.source(sourceBuilder);
       try {
           SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
           SearchHits hits = response.getHits();
           JSONArray jsonArray = new JSONArray();
           for (SearchHit hit : hits) {
               String sourceAsString = hit.getSourceAsString();
               JSONObject jsonObject = JSON.parseObject(sourceAsString);
               jsonArray.add(jsonObject);
           }
           return new ResponseBean(200, "查询成功", jsonArray);
       } catch (IOException e) {
           e.printStackTrace();
           return new ResponseBean(10001, "查询失败", null);
       }
   }

    /**
     * 聚合查询
     * @return
     */
    public ResponseBean testESFindAgg() {
        SearchRequest searchRequest = new SearchRequest(EsContants.INDEX_NAME);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("by_age").field("age");
        sourceBuilder.aggregation(termsAggregationBuilder);

        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            Map<String, Aggregation> stringAggregationMap = aggregations.asMap();
            ParsedLongTerms parsedLongTerms = (ParsedLongTerms) stringAggregationMap.get("by_age");
            List<? extends Terms.Bucket> buckets = parsedLongTerms.getBuckets();
            Map<Integer, Long> map = new HashMap<>();
            for (Terms.Bucket bucket : buckets) {
                long docCount = bucket.getDocCount();//个数
                Number keyAsNumber = bucket.getKeyAsNumber();//年龄
                System.err.println(keyAsNumber + "岁的有" + docCount + "个");
                map.put(keyAsNumber.intValue(), docCount);
            }
            return new ResponseBean(200, "查询成功", map);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }



    /**
     * 删除Id
     * @param id
     * @param indexName
     * @return
     */
    public ResponseBean testESDelete(@RequestParam String id, @RequestParam String indexName) {
        DeleteRequest deleteRequest = new DeleteRequest(indexName);
        deleteRequest.id(id);
        try {
            DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
            if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                return new ResponseBean(1001, "删除失败", null);
            } else {
                return new ResponseBean(200, "删除成功", null);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return new ResponseBean(1003, "删除异常", null);
        }
    }
    /**
     * 搜索入口 搜索结果处理
     */
  /*  public Page<User> searchContent(Pageable pageable, String content) throws IOException{
        SearchResponse searchResponse = searchHighlight(pageable.getPageSize() * pageable.getPageNumber(), pageable.getPageSize(), content, "account", "name");
        List<User> list = Arrays.stream(searchResponse.getHits().getHits()).map(this::toHighLightUser).collect(Collectors.toList());
        return new PageImpl<>(list, pageable, searchResponse.getHits().getTotalHits().value);
    }*/

    /**
     * 构建SearchRequest, SearchSourceBuilder , 执行search方法
     */
    private SearchResponse searchDocument(HighlightBuilder highlightBuilder, QueryBuilder queryBuilder, int from, int size) throws IOException{
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if (!ObjectUtils.isEmpty(highlightBuilder)){
            searchSourceBuilder.highlighter(highlightBuilder);
        }
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.from(from);
        searchSourceBuilder.size(size);
        searchRequest.source(searchSourceBuilder);
        return client.search(searchRequest, RequestOptions.DEFAULT);
    }
    /**
     * 构建高亮字段 进行 多字段模糊查询
     */
    private SearchResponse searchHighlight(int from, int size, String content, String ... fields) throws IOException{
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        for (String field : fields){
            highlightBuilder.field(makeHighlightContent(field));
        }
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //自定义条件    multi_match 跨多个 field 查询，表示查询分词在任意一个字段中 也算匹配的结果。
        boolQueryBuilder.should(QueryBuilders.multiMatchQuery(content, fields).fuzziness(Fuzziness.AUTO));
        //match_phrase  不分词
        //QueryBuilders.multiMatchQuery(content, fields).operator(Operator.AND);//multi_match 跨多个 field 查询，表示查询分词必须出现在相同字段中。
        boolQueryBuilder.should(QueryBuilders.wildcardQuery("account", "*" + content + "*"));
        boolQueryBuilder.should(QueryBuilders.wildcardQuery("name", "*" + content + "*"));
        return searchDocument(highlightBuilder, boolQueryBuilder , from , size);
    }
    /**
     * 封装高亮查询字段
     */
    private HighlightBuilder.Field makeHighlightContent(String fieldName){
        HighlightBuilder.Field highlightContent = new HighlightBuilder.Field(fieldName);
        highlightContent.preTags("<span style=\"color:red\">");
        highlightContent.postTags("</span>");
        return highlightContent;
    }
    /**
     * 批量导入
     * @param indexName
     * @param isAutoId 使用自动id 还是使用传入对象的id
     * @param source
     * @return
     * @throws IOException
     */
    public BulkResponse importAll(String indexName, boolean isAutoId, String ... source) throws IOException{
        if (0 == source.length){
            //todo 抛出异常 导入数据为空
        }
        BulkRequest request = new BulkRequest();
        if (isAutoId) {
            for (String s : source) {
                request.add(new IndexRequest(indexName).source(s, XContentType.JSON));
            }
        } else {
            for (String s : source) {
                request.add(new IndexRequest(indexName).id(JSONObject.parseObject(s).getString("id")).source(s, XContentType.JSON));
            }
        }
         BulkResponse bulkAddResponse = client.bulk(request, RequestOptions.DEFAULT);

// 批量更新
        List<User> testsList = new ArrayList<>();

        BulkRequest bulkUpdateRequest = new BulkRequest();
        for (int i = 0; i < testsList.size(); i++) {
            User user = testsList.get(i);
            user.setName(user.getName() + " updated");
            UpdateRequest updateRequest = new UpdateRequest(EsContants.INDEX_NAME, user.getId().toString());
            updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);
            bulkUpdateRequest.add(updateRequest);
        }
        BulkResponse bulkUpdateResponse = client.bulk(bulkUpdateRequest, RequestOptions.DEFAULT);
        System.out.println("bulkUpdate: " + JSON.toJSONString(bulkUpdateResponse));
        search(EsContants.INDEX_NAME, "updated");


// 批量删除
        BulkRequest bulkDeleteRequest = new BulkRequest();
        for (int i = 0; i < testsList.size(); i++) {
            User user = testsList.get(i);
            DeleteRequest deleteRequest = new DeleteRequest(EsContants.INDEX_NAME, user.getId().toString());
            bulkDeleteRequest.add(deleteRequest);
        }
        BulkResponse bulkDeleteResponse = client.bulk(bulkDeleteRequest, RequestOptions.DEFAULT);
        System.out.println("bulkDelete: " + JSON.toJSONString(bulkDeleteResponse));
        search(EsContants.INDEX_NAME, "this");

         return bulkAddResponse;
    }

    /**
     * match + match_phrase + slop 组合查询，使查询结果更加精准和结果更多
     * 但是 match_phrase 性能没有 match 好，所以一般需要先用 match 第一步进行过滤，然后在用 match_phrase 进行进一步匹配，并且重新打分，这里又用到了：rescore，window_size 表示对前 10 个进行重新打分
     GET /product_index/product/_search
     {
     "query": {
     "bool": {
     "must": {
     "match": {
     "product_name": {
     "query": "PHILIPS HX6730"
     }
     }
     },
     "should": {
     "match_phrase": {
     "product_name": {
     "query": "PHILIPS HX6730",
     "slop": 10
     }
     }
     }
     }
     }
     }

     GET /product_index/product/_search
     {
     "query": {
     "match": {
     "product_name": "PHILIPS HX6730"
     }
     },
     "rescore": {
     "window_size": 10,
     "query": {
     "rescore_query": {
     "match_phrase": {
     "product_name": {
     "query": "PHILIPS HX6730",
     "slop": 10
     }
     }
     }
     }
     }
     }
     原文链接：https://blog.csdn.net/jiaminbao/article/details/80105636
     * @param index
     * @param name
     * @throws IOException
     */
    public void search(String index, String name) throws IOException {
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        boolBuilder.must(QueryBuilders.matchQuery("name", name)); // 这里可以根据字段进行搜索，must表示符合条件的，相反的mustnot表示不符合条件的
        //slop = 2 表示中间如果间隔 2 个单词以内也算是匹配的结果
        // 其实也不能称作间隔，应该说是移位，查询的关键字分词后移动多少位可以跟 doc 内容匹配，移动的次数就是 slop。所以 HX6730 PHILIPS 其实也是可以匹配到 doc 的
        // QueryBuilders.matchPhraseQuery("name", name).slop(2);
        // boolBuilder.must(QueryBuilders.matchQuery("id", tests.getId().toString()));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolBuilder);
        sourceBuilder.from(0);
        sourceBuilder.size(100); // 获取记录数，默认10
        sourceBuilder.fetchSource(new String[]{"id", "name"}, new String[]{}); // 第一个是获取字段，第二个是过滤的字段，默认获取全部
        SearchRequest searchRequest = new SearchRequest(index);
       // searchRequest.types(type);
        searchRequest.source(sourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("search: " + JSON.toJSONString(response));
        SearchHits hits = response.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            System.out.println("search -> " + hit.getSourceAsString());
        }

    }


    public void searchAreaByLatAndLon(@RequestParam("token") String token,
                                           @RequestParam("lat") Double lat,
                                           @RequestParam("lon") Double lon) throws Exception {

        //设定搜索半径

        GeoDistanceQueryBuilder queryBuilder = QueryBuilders.geoDistanceQuery("location")
                .point(lat, lon)
                .distance(1500, DistanceUnit.KILOMETERS)
                .geoDistance(GeoDistance.PLANE);

        //按距离排序
        GeoDistanceSortBuilder sort = SortBuilders.geoDistanceSort("location", lat, lon);
        sort.order(SortOrder.ASC);
        sort.point(lat, lon);
        //构建检索
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .from(0)
                .size(20)
                .query(queryBuilder )
                .sort(sort);
        //SearchHits searchHits = elasearchService.searchDocument(ElsIndexEnums.rz_area.getIndex(), searchSourceBuilder);


        //创建搜索构建者
        SearchRequest searchRequest = new SearchRequest(EsContants.INDEX_NAME); //索引
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(0); //设置确定结果要从哪个索引开始搜索的from选项，默认为0
        sourceBuilder.size(100); //设置确定搜素命中返回数的size选项，默认为10
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)); //设置一个可选的超时，控制允许搜索的时间。
        searchRequest.source(sourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits searchHits = response.getHits();

        ArrayList<Map> arrayList = new ArrayList<Map>();

        {//控制台测试显示！生产删除！
            searchHits.forEach(hit->{
                Map map = new HashMap();
                map.put("complete_path", hit.getSourceAsMap().get("complete_path"));
                //获取距离值，并保留两位小数点
                BigDecimal geoDis = new BigDecimal((Double) hit.getSortValues()[0]);
                //坐标
                Object location2 = hit.getSourceAsMap().get("location");
                Map<String, Object> hitMap = hit.getSourceAsMap();
                //计算距离
                hitMap.put("geoDistance", geoDis.setScale(0, BigDecimal.ROUND_HALF_DOWN));
                map.put("range", hit.getSourceAsMap().get("geoDistance"));
                arrayList.add(map);
                System.out.println(hit.getSourceAsMap().get("complete_path") + "的坐标：" +
                        location2 + "与我的距离" +
                        hit.getSourceAsMap().get("geoDistance") +
                        DistanceUnit.METERS.toString());
            });
        }

        Map<String, Object> map = new HashMap<>();
        if (searchHits.getHits().length > 0) {
            map = searchHits.getHits()[0].getSourceAsMap();
        }
        //return AppResDto.success(arrayList);
        //return AppResDto.success(map);
    }


    public String searchBigAll(String content) throws IOException {
        //创建检索请求
        SearchRequest searchRequest = new SearchRequest(EsContants.INDEX_NAME); //索引


        //创建搜索构建者
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();

        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("fields.entity_id", "319");//这里可以根据字段进行搜索，must表示符合条件的，相反的mustnot表示不符合条件的
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("fields_timestamp"); //新建range条件
        rangeQueryBuilder.gte("2019-03-21T08:24:37.873Z"); //开始时间
        rangeQueryBuilder.lte("2019-03-21T08:24:37.873Z"); //结束时间
        //boolBuilder.must(rangeQueryBuilder);
        //boolBuilder.must(matchQueryBuilder);
        //sourceBuilder.query(boolBuilder); //设置查询，可以是任何类型的QueryBuilder。


        String[] includeFields = new String[] {"fields.port","fields.entity_id","fields.message"};
        String[] excludeFields =  new String[] {""};
        if (!CollectionUtils.isEmpty(includeFields) || !CollectionUtils.isEmpty(excludeFields)) {
            sourceBuilder.fetchSource(includeFields, excludeFields); //返回和排除列 第一个是获取字段，第二个是过滤的字段，默认获取全部
        }

        if (!StringUtils.isEmpty(content)){
            //自定义组合查询  boost是设置权重 /查询结果status为4的会被优先展示其次coutent，再次fields_timestamp
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            TermQueryBuilder termQuery = QueryBuilders.termQuery("status", 4).boost(8);
            //wildcard通配符查询 性能较差不建议使用 ?：任意字符
            // WildcardQueryBuilder termQuery1 = QueryBuilders.wildcardQuery("appli_name", "1?3");
            MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("coutent",content)
                    .fuzziness(Fuzziness.AUTO).boost(1); //模糊匹配
            boolQueryBuilder.must(termQuery).must(queryBuilder).must(rangeQueryBuilder).must(matchQueryBuilder);
            //.must(termQuery1);
            sourceBuilder.query(boolQueryBuilder);
        }
        sourceBuilder.from(0); //设置确定结果要从哪个索引开始搜索的from选项，默认为0
        sourceBuilder.size(100); //设置确定搜素命中返回数的size选项，默认为10
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)); //设置一个可选的超时，控制允许搜索的时间。
        sourceBuilder.sort(new FieldSortBuilder("id").order(SortOrder.ASC)); //根据自己的需求排序
        // 排序
        //FieldSortBuilder fsb = SortBuilders.fieldSort("date");
        //fsb.order(SortOrder.DESC);
        //sourceBuilder.sort(fsb);

        searchRequest.source(sourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();  //SearchHits提供有关所有匹配的全局信息，例如总命中数或最高分数：
        SearchHit[] searchHits = hits.getHits();
        // List<User> list = Arrays.stream(response.getHits().getHits()).map(this::toHig).collect(Collectors.toList());
        for (SearchHit hit : searchHits) {
            log.info("search -> {}",hit.getSourceAsString());
        }
        return Arrays.toString(searchHits);
    }

    /**
     * 大量数据查询
     * @param lastTime
     * @param nowTime
     * @param scrollTimeOut
     * @return
     * @throws IOException
     */
    public  List<SearchHit> scrollSearchAll(long lastTime,long nowTime,Long scrollTimeOut) throws IOException {
        //设定滚动时间间隔,60秒,不是处理查询结果的所有文档的所需时间
        //游标查询的过期时间会在每次做查询的时候刷新，所以这个时间只需要足够处理当前批的结果就可以了
        final  Scroll scroll = new Scroll(TimeValue.timeValueSeconds(scrollTimeOut));
        SearchRequest searchRequest = new SearchRequest(EsContants.INDEX_NAME);
        searchRequest.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("intercept_time").gte(lastTime).lte(nowTime));
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.size(5000); //设定每次返回多少条数据
        searchSourceBuilder.fetchSource(new String[]{"nid"},null);//设置返回字段和排除字段
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId;


        List<SearchHit> resultSearchHit = new ArrayList<SearchHit>();
        do {
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                //获取需要数据
                resultSearchHit.add(hit);
            }
            //每次循环完后取得scrollId,用于记录下次将从这个游标开始取数
            scrollId = searchResponse.getScrollId();
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            try {
                //进行下次查询
                searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                log.error("获取数据错误2 ->", e);
            }
        } while (searchResponse.getHits().getHits().length != 0);
        //及时清除es快照，释放资源
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        //也可以选择setScrollIds()将多个scrollId一起使用
        clearScrollRequest.addScrollId(scrollId);
        client.clearScroll(clearScrollRequest,RequestOptions.DEFAULT);
        return resultSearchHit;
    }
    public void testScroll() {
        // 初始化scroll
        // 设定滚动时间间隔
        // 这个时间并不需要长到可以处理所有的数据，仅仅需要足够长来处理前一批次的结果。每个 scroll 请求（包含 scroll 参数）设置了一个新的失效时间。
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));//设定滚动时间间隔
        SearchRequest searchRequest = new SearchRequest(EsContants.INDEX_NAME); // 新建索引搜索请求
        searchRequest.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("intercept_time").gte("2019-12-01").lte("2019-12-20"));

        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.size(5000); //设定每次返回多少条数据
        searchSourceBuilder.fetchSource(new String[]{"nid"},null);//设置返回字段和排除字段
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        int page = 0 ;
        File outFile = new File("E://cater_nid.csv");//写出的CSV文件
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));

            SearchHit[] searchHits = searchResponse.getHits().getHits();
            page++;
            System.out.println("-----第"+ page +"页-----");
            for (SearchHit searchHit : searchHits) {
                //System.out.println(searchHit.getSourceAsString());
                String sourceAsString = searchHit.getSourceAsString();
                NID t = JSON.parseObject(sourceAsString, NID.class);
                writer.write(t.getNid());
                writer.newLine();
            }

            //遍历搜索命中的数据，直到没有数据
            String scrollId = searchResponse.getScrollId();
            while (searchHits != null && searchHits.length > 0) {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                try {
                    searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
                if (searchHits != null && searchHits.length > 0) {
                    System.out.println("-----下一页-----");
                    page++;
                    System.out.println("-----第"+ page +"页-----");
                    for (SearchHit searchHit : searchHits) {
                        //System.out.println(searchHit.getSourceAsString());
                        String sourceAsString = searchHit.getSourceAsString();
                        NID t = JSON.parseObject(sourceAsString, NID.class);
                        writer.write(t.getNid());
                        writer.newLine();
                    }
                }
            }
            //清除滚屏
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);//也可以选择setScrollIds()将多个scrollId一起使用
            ClearScrollResponse clearScrollResponse = null;
            try {
                clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
            boolean succeeded = clearScrollResponse.isSucceeded();
            System.out.println("succeeded:" + succeeded);

            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 统计姓名为张三的 平均年龄
     * select avg(age) age from user where name=张三
     *
     * @return List<User>
     */
    public long countAge(String name) {
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            QueryBuilder names = QueryBuilders.termQuery("name", name);
            AggregationBuilder avgAge = AggregationBuilders.avg("avg_age").field("age");
            searchSourceBuilder.aggregation(avgAge);
            searchSourceBuilder.query(names);
            searchSourceBuilder.size(0);
            SearchRequest searchRequest = new SearchRequest(EsContants.INDEX_NAME).source(searchSourceBuilder);
            SearchResponse response = client.search(searchRequest,RequestOptions.DEFAULT);
            ParsedAvg avgAggregationBuilder = response.getAggregations().get("avg_age");
            return (long) avgAggregationBuilder.value();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 统计姓名为张三的 年龄总和
     * select sum(age) age from user where name=张三
     *
     * @return List<User>
     */
    public long sumAge(String name) {
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            QueryBuilder names = QueryBuilders.termQuery("name", name);
            AggregationBuilder sumAge = AggregationBuilders.sum("total_sum").field("age");
            searchSourceBuilder.aggregation(sumAge);
            searchSourceBuilder.query(names);
            searchSourceBuilder.size(0);
            SearchRequest searchRequest = new SearchRequest(EsContants.INDEX_NAME).source(searchSourceBuilder);
            SearchResponse response = client.search(searchRequest,RequestOptions.DEFAULT);
            ParsedSum.SingleValue avgAggregationBuilder = response.getAggregations().get("avg_age");
            return (long) avgAggregationBuilder.value();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    /**
     * 求出最小值
     * select min(*)
     *
     * @param field
     * @return
     */
    private AggregationBuilder min(String field) {
        return AggregationBuilders.min("min").field(field);
    }
    /**
     * 求出最大值
     * select min(*)
     *
     * @param field
     * @return
     */
    private AggregationBuilder max(String field) {
        return AggregationBuilders.max("max").field(field);
    }
    /**
     * count统计个数
     *
     * @param field
     * @return
     */
    private ValueCountAggregationBuilder count(String field) {
        return AggregationBuilders.count("count_name").field(field);

    }

    /**
     * 分组查询 AggregationBuilders.terms("分组之后的名称").field("要分组的名称");
     */
    public void teram() {
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            //根据姓名进行分组统计个数
            TermsAggregationBuilder field = AggregationBuilders.terms("terms_name").field("name");
            ValueCountAggregationBuilder countField = AggregationBuilders.count("count_name").field("name");
            field.subAggregation(countField);
            searchSourceBuilder.aggregation(field);
            SearchRequest searchRequest = new SearchRequest(EsContants.INDEX_NAME).source(searchSourceBuilder);
            SearchResponse response = client.search(searchRequest,RequestOptions.DEFAULT);
            //分组在es中是分桶
            ParsedStringTerms termsName = response.getAggregations().get("terms_name");
            List<? extends Terms.Bucket> buckets = termsName.getBuckets();
            buckets.forEach(naem -> {
                String key = (String) naem.getKey();
                ParsedValueCount countName = naem.getAggregations().get("count_name");
                double value = countName.value();
                log.info("name , count {} {}", key, value);
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }




    /**
     *  获取附近的人
     * @param lat 纬度
     * @param lon 经度
     * 于谦的坐标，查询距离王小丽1米到1000米的所有人
     * @throws IOException
     */
    public  void testGetNearbyPeople(double lat, double lon) throws Exception  {
        //lat = 39.929986;
        //lon = 116.395645;
        //初始化数据
        esUtil.addIndexData();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0).size(1000);//1000人
        SearchRequest searchRequest = new SearchRequest(EsContants.INDEX_NAME).source(searchSourceBuilder);

//        FilterBuilder builder = geoDistanceRangeFilter("location").point(lon, lat).from("1m").to("100m").optimizeBbox("memory").geoDistance(GeoDistance.PLANE);
        GeoDistanceQueryBuilder location1 = QueryBuilders.geoDistanceQuery("location").point(lat,lon).distance(80,DistanceUnit.METERS);
        searchSourceBuilder.postFilter(location1);
        // 获取距离多少公里 这个才是获取点与点之间的距离的
        GeoDistanceSortBuilder sort = SortBuilders.geoDistanceSort("location",lat,lon);
        sort.unit(DistanceUnit.METERS);
        sort.order(SortOrder.ASC);
        sort.point(lat,lon);
        searchSourceBuilder.sort(sort);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();
        SearchHit[] searchHists = hits.getHits();
        // 搜索耗时
        long usetime = response.getTook().getMillis()/1000;
        System.out.println("王小丽附近的人(" + hits.getTotalHits() + "个)，耗时("+usetime+"秒)：");
        for (SearchHit hit : searchHists) {
            String name = (String)hit.getSourceAsMap().get("name");
            List<Double> location = (List<Double>)hit.getSourceAsMap().get("location");
            // 获取距离值，并保留两位小数点
            BigDecimal geoDis = new BigDecimal((Double) hit.getSortValues()[0]);
            Map<String, Object> hitMap = hit.getSourceAsMap();
            // 在创建MAPPING的时候，属性名的不可为geoDistance。
            hitMap.put("geoDistance", geoDis.setScale(0, BigDecimal.ROUND_HALF_DOWN));
            System.out.println(name+"的坐标："+location + "他距离王小丽" + hit.getSourceAsMap().get("geoDistance") + DistanceUnit.METERS.toString());
        }

    }
}