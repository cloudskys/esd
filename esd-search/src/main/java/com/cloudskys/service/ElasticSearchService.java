package com.cloudskys.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cloudskys.domain.ResponseBean;
import com.cloudskys.domain.User;
import com.cloudskys.untils.EsUtil;
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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
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

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * QueryBuilders.termQuery("key", obj) 完全匹配
 * QueryBuilders.termsQuery("key", obj1, obj2..)   一次匹配多个值
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
    private static final String INDEX_NAME = "test";
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
        if (!existsIndex(INDEX_NAME)) {
            createIndex(INDEX_NAME);
        }

        IndexRequest request = new IndexRequest(INDEX_NAME);
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
        GetRequest getRequest = new GetRequest(index, type, userVo.getId().toString());
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println("exists: " + exists);
        return exists;
    }

    //查询单个文档
    public Object getDoc(String id) {
        GetRequest getRequest = new GetRequest(INDEX_NAME, id);
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
        DeleteRequest request = new DeleteRequest(INDEX_NAME, id);
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
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME); //索引


        //创建搜索构建者
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();

        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("fields.entity_id", "319");//这里可以根据字段进行搜索，must表示符合条件的，相反的mustnot表示不符合条件的
        // RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("fields_timestamp"); //新建range条件
        // rangeQueryBuilder.gte("2019-03-21T08:24:37.873Z"); //开始时间
        // rangeQueryBuilder.lte("2019-03-21T08:24:37.873Z"); //结束时间
        // boolBuilder.must(rangeQueryBuilder);
        //boolBuilder.must(matchQueryBuilder);
        //sourceBuilder.query(boolBuilder); //设置查询，可以是任何类型的QueryBuilder。
        sourceBuilder.from(0); //设置确定结果要从哪个索引开始搜索的from选项，默认为0
        sourceBuilder.size(100); //设置确定搜素命中返回数的size选项，默认为10
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)); //设置一个可选的超时，控制允许搜索的时间。
        // sourceBuilder.sort(new FieldSortBuilder("id").order(SortOrder.ASC)); //根据自己的需求排序

        sourceBuilder.fetchSource(new String[] {"fields.port","fields.entity_id","fields.message"}, new String[] {}); //第一个是获取字段，第二个是过滤的字段，默认获取全部
        if (!StringUtils.isEmpty(content)){
            //自定义组合查询
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            TermQueryBuilder termQuery = QueryBuilders.termQuery("status", 4);
            MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("coutent",content)
                    .fuzziness(Fuzziness.AUTO); //模糊匹配
            boolQueryBuilder.must(termQuery).must(queryBuilder);
            sourceBuilder.query(boolQueryBuilder);
        }

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
        UpdateRequest updateRequest = new UpdateRequest(INDEX_NAME, id);
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
       SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
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
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
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
        //自定义条件
        boolQueryBuilder.should(QueryBuilders.multiMatchQuery(content, fields).fuzziness(Fuzziness.AUTO));
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
            UpdateRequest updateRequest = new UpdateRequest(INDEX_NAME, user.getId().toString());
            updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);
            bulkUpdateRequest.add(updateRequest);
        }
        BulkResponse bulkUpdateResponse = client.bulk(bulkUpdateRequest, RequestOptions.DEFAULT);
        System.out.println("bulkUpdate: " + JSON.toJSONString(bulkUpdateResponse));
        search(INDEX_NAME, "updated");


// 批量删除
        BulkRequest bulkDeleteRequest = new BulkRequest();
        for (int i = 0; i < testsList.size(); i++) {
            User user = testsList.get(i);
            DeleteRequest deleteRequest = new DeleteRequest(INDEX_NAME, user.getId().toString());
            bulkDeleteRequest.add(deleteRequest);
        }
        BulkResponse bulkDeleteResponse = client.bulk(bulkDeleteRequest, RequestOptions.DEFAULT);
        System.out.println("bulkDelete: " + JSON.toJSONString(bulkDeleteResponse));
        search(INDEX_NAME, "this");

         return bulkAddResponse;
    }
    public void search(String index, String name) throws IOException {
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        boolBuilder.must(QueryBuilders.matchQuery("name", name)); // 这里可以根据字段进行搜索，must表示符合条件的，相反的mustnot表示不符合条件的
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
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME); //索引
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


}