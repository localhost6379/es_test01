package cn.king;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootVersion;
import org.springframework.core.SpringVersion;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author: wjl@king.cn
 * @time: 2022/7/31 15:59
 * @version: 1.0.0
 * @description:
 */
public class RestHighLevelClientTest extends ElasticSearchTest {

    // springData会为我们创建两个操作es的对象，一个是ElasticsearchOperations，一个是RestHighLevelClient
    // ElasticsearchOperations是纯面向对象方式操作es，不能执行dsl语句。RestHighLevelClient是类似kibana的restful方式操作es，
    // RestHighLevelClient用的最多，Spring官网主推，上手最容易。推荐使用
    // @Autowired
    // private ElasticsearchOperations elasticsearchOperations;

    @Autowired
    private RestHighLevelClient client;

    // SpringDataES和ES和Spring的版本对应：https://docs.spring.io/spring-data/elasticsearch/docs/current/reference/html/#preface.versions
    @Test
    public void getSpringVersion() {
        String springbootVersion = SpringBootVersion.getVersion();
        String springVersion = SpringVersion.getVersion();
        // 2.3.7.RELEASE
        System.out.println(springbootVersion);
        // 5.2.12.RELEASE
        System.out.println(springVersion);
    }

    /**
     * @description: 创建索引和映射
     */
    @Test
    public void test01() throws Exception {
        // 参数1：创建索引请求对象 参数2：请求配置对象
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("product");
        // 参数1：映射 参数2：数据类型。映射我们一般会在kibana中书写好之后拿到程序中。也可以预先在kibana中创建索引和映射。
        createIndexRequest.mapping("{\n" +
                "    \"properties\": {\n" +
                "      \"title\":{\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"price\":{\n" +
                "        \"type\": \"double\"\n" +
                "      },\n" +
                "      \"create_time\":{\n" +
                "        \"type\": \"date\"\n" +
                "      },\n" +
                "      \"desc\":{\n" +
                "        \"type\": \"text\",\n" +
                "        \"analyzer\": \"ik_max_word\"\n" +
                "      }\n" +
                "    }\n" +
                "  }", XContentType.JSON);
        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        // 返回的对象实际上和kibana中相同。
        // {"acknowledged":true,"fragment":false,"shardsAcknowledged":true}
        System.out.println(JSON.toJSONString(createIndexResponse));
        // 使用完建议关闭资源。因为是一个rest
        client.close();
    }

    /**
     * 删除索引
     *
     * @throws Exception
     */
    @Test
    public void test02() throws Exception {
        AcknowledgedResponse acknowledgedResponse = client.indices().delete(new DeleteIndexRequest("product"), RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(acknowledgedResponse));
    }

    /**
     * 索引一条文档
     * <p>
     * # 索引一条文档
     * PUT /product/_doc/1
     * {
     * "title":"小浣熊",
     * "price":1.5,
     * "create_time":"2022-02-02",
     * "desc":"小浣熊很好吃"
     * }
     */
    @Test
    public void test03() throws Exception {
        IndexRequest indexRequest = new IndexRequest("product");
        indexRequest
                // 手动指定文档的id。可以不.id("1")，此时自动生成id
                .id("1")
                // 指定文档的数据
                .source("{\n" +
                        "  \"title\": \"小浣熊\",\n" +
                        "  \"price\": 1.5,\n" +
                        "  \"create_time\": \"2022-02-02\",\n" +
                        "  \"desc\": \"小浣熊很好吃\"\n" +
                        "}\n", XContentType.JSON);
        // 参数：索引请求对象 请求配置对象
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(indexResponse));
        System.out.println(indexResponse.getResult());
    }

    /**
     * 更新文档
     * <p>
     * # 修改文档
     * POST /product/_doc/1/_update
     * {
     * "doc":{
     * "title":"大浣熊"
     * }
     * }
     */
    @Test
    public void test04() throws Exception {
        UpdateRequest updateRequest = new UpdateRequest("product", "1");
        updateRequest.doc("{\n" +
                "    \"title\":\"大浣熊\"\n" +
                "  }", XContentType.JSON);
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(updateResponse.status());
        client.close();
    }

    /**
     * 删除文档
     * <p>
     * DELETE /product/_doc/1
     */
    @Test
    public void test05() throws Exception {
        DeleteResponse deleteResponse = client.delete(new DeleteRequest("product", "1"), RequestOptions.DEFAULT);
        System.out.println(deleteResponse.getResult()); // DELETED
        System.out.println(deleteResponse.status()); // OK 
    }

    /**
     * 基于id查询文档
     * <p>
     * GET /product/_doc/7pV3VIIB_yz1LgAMtVng
     */
    @Test
    public void test06() throws Exception {
        GetResponse getResponse = client.get(new GetRequest("product", "7pV3VIIB_yz1LgAMtVng"), RequestOptions.DEFAULT);
        System.out.println(getResponse.getId());
        Map<String, Object> source = getResponse.getSource();
        source.forEach((k, v) -> System.out.println(k + "--" + v));
        // {"title":"小浣熊","price":1.5,"create_time":"2022-02-02","desc":"小浣熊很好吃"}
        System.out.println(getResponse.getSourceAsString());
    }

    /**
     * 查询所有
     * 
     * GET /product/_search
     * {
     *   "query": {
     *     "match_all": {}
     *   }
     * }
     */
    @Test
    public void test07() throws Exception{
        // 索引参数是可变参数
        SearchRequest searchRequest = new SearchRequest("product");
        // 指定查询条件 
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 查询所有
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        // 参数：搜索请求对象 请求配置参数 
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("总条数：" + searchResponse.getHits().getTotalHits().value);
        System.out.println("最大得分：" + searchResponse.getHits().getMaxScore());
        // 获取结果
        SearchHit[] hits = searchResponse.getHits().getHits();
        Stream.of(hits).forEach(documentFields -> System.out.println(documentFields.getSourceAsString()));
    }

    /**
     * term 关键词精确查询 
     */
    @Test
    public void test08() throws Exception{
        SearchRequest searchRequest = new SearchRequest("product");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("desc", "浣熊"));
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        Stream.of(hits).forEach(documentFields -> System.out.println(documentFields.getSourceAsString()));
    }

    /**
     * 一些查询 
     */
    @Test
    public void test09() throws Exception{
        // term 关键词查询 
        this.query(QueryBuilders.termQuery("desc","浣熊"));
        // range 范围查询 
        this.query(QueryBuilders.rangeQuery("price").gte(0).lte(10));
        // prefix 前缀查询 
        this.query(QueryBuilders.prefixQuery("desc", "小"));
        // wildcard 通配符查询 
        this.query(QueryBuilders.wildcardQuery("title", "小浣熊*"));
        // ids 多id查询
        this.query(QueryBuilders.idsQuery().addIds("1", "2").addIds("3"));
        // multi_match 多字段查询。multi_match查询，如果查询的字段分词，那么就会把查询条件分词处理。如果查询的字段不分词，那么查询条件不分词。
        this.query(QueryBuilders.multiMatchQuery("非常不错", "title").field("desc"));
    }

    /**
     * 分页处理 
     * 
     * 默认es分页只会返回前10条记录 
     * 
     * GET /product/_search
     * {
     *   "query": {
     *     "match_all": {}
     *   },
     *   "from": 0,
     *   "size": 20
     * }
     */
    @Test
    public void test10() throws Exception{
        // 索引参数是可变参数
        SearchRequest searchRequest = new SearchRequest("product");
        // 指定查询条件 
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 查询所有
        searchSourceBuilder.query(QueryBuilders.matchAllQuery())
                .from(0) // start = (page-1)*size 
                .size(20);
        searchRequest.source(searchSourceBuilder);
        // 参数：搜索请求对象 请求配置参数 
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("总条数：" + searchResponse.getHits().getTotalHits().value);
        System.out.println("最大得分：" + searchResponse.getHits().getMaxScore());
        // 获取结果
        SearchHit[] hits = searchResponse.getHits().getHits();
        Stream.of(hits).forEach(documentFields -> System.out.println(documentFields.getSourceAsString()));
    }

    /**
     * 
     * 按照指定字段排序 
     * 
     * GET /product/_search
     * {
     *   "query": {
     *     "match_all": {}
     *   },
     *   "from": 0,
     *   "size": 20,
     *   "sort": [
     *     {
     *       "price": {
     *         "order": "desc"
     *       }
     *     }
     *   ]
     * }
     */
    @Test
    public void test11() throws Exception{
        // 索引参数是可变参数
        SearchRequest searchRequest = new SearchRequest("product");
        // 指定查询条件 
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 查询所有
        searchSourceBuilder.query(QueryBuilders.matchAllQuery())
                .from(0) // start = (page-1)*size 
                .size(20)
                .sort("price", SortOrder.DESC); // 指定排序方式。如果自己做了排序，那么es查询出的分数为NaN，计算分数没有意义了 
        searchRequest.source(searchSourceBuilder);
        // 参数：搜索请求对象 请求配置参数 
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("总条数：" + searchResponse.getHits().getTotalHits().value);
        System.out.println("最大得分：" + searchResponse.getHits().getMaxScore());
        // 获取结果
        SearchHit[] hits = searchResponse.getHits().getHits();
        Stream.of(hits).forEach(documentFields -> System.out.println(documentFields.getSourceAsString()));
    }

    /**
     * 返回指定字段 
     * 
     * GET /product/_search
     * {
     *   "query": {
     *     "match_all": {}
     *   },
     *   "from": 0,
     *   "size": 20,
     *   "sort": [
     *     {
     *       "price": {
     *         "order": "desc"
     *       }
     *     }
     *   ],
     *   "_source": ["title","desc"]
     * }
     */
    @Test
    public void test12() throws Exception{
        // 索引参数是可变参数
        SearchRequest searchRequest = new SearchRequest("product");
        // 指定查询条件 
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 查询所有
        searchSourceBuilder.query(QueryBuilders.matchAllQuery())
                .from(0) // start = (page-1)*size 
                .size(20)
                .sort("price", SortOrder.DESC) // 指定排序方式。如果自己做了排序，那么es查询出的分数为NaN，计算分数没有意义了 
                // 参数1：包含字段数组 参数2：排除字段数组 
                .fetchSource(new String[]{"title"}, new String[]{});
        searchRequest.source(searchSourceBuilder);
        // 参数：搜索请求对象 请求配置参数 
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("总条数：" + searchResponse.getHits().getTotalHits().value);
        System.out.println("最大得分：" + searchResponse.getHits().getMaxScore());
        // 获取结果
        SearchHit[] hits = searchResponse.getHits().getHits();
        Stream.of(hits).forEach(documentFields -> System.out.println(documentFields.getSourceAsString()));
    }

    /**
     * 高亮查询 
     * # 高亮查询。高亮不能使用match_all
     * GET /product/_search
     * {
     *   "query": {
     *     "term": {
     *       "desc": {
     *         "value": "好吃"
     *       }
     *     }
     *   },
     *   "from": 0,
     *   "size": 20,
     *   "sort": [
     *     {
     *       "price": {
     *         "order": "desc"
     *       }
     *     }
     *   ],
     *   "_source": ["title","desc"],
     *   "highlight": {
     *     "fields": {"desc": {},"title": {}},
     *     "pre_tags": ["<span style='color:red'>"],
     *    "post_tags": ["</span>"],
     *    "require_field_match": "false"
     *   }
     * }
     * # es只能对分词的字段进行高亮，不分词的字段是不能高亮的
     * # es默认只能对搜索的字段进行高亮，上述只能对desc字段进行高亮，如果想对所有字段进行高亮，需要设置"require_field_match": "false"
     */
    @Test
    public void test13() throws Exception {
        // 索引参数是可变参数
        SearchRequest searchRequest = new SearchRequest("product");
        // 指定查询条件 
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        
        // 创建高亮器 
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder
                .requireFieldMatch(Boolean.FALSE)
                // .field("*")
                .field("desc")
                .field("title")
                .preTags("<span style='color:red'>")
                .postTags("</span>");

        // 高亮查询不能查询全部 
        searchSourceBuilder.query(QueryBuilders.termQuery("desc", "好吃"))
                .from(0) // start = (page-1)*size 
                .size(20)
                .sort("price", SortOrder.DESC) // 指定排序方式。如果自己做了排序，那么es查询出的分数为NaN，计算分数没有意义了 
                // 参数1：包含字段数组 参数2：排除字段数组 
                .fetchSource(new String[]{"title", "desc"}, new String[]{})
                .highlighter(highlightBuilder);
                
        searchRequest.source(searchSourceBuilder);
        // 参数：搜索请求对象 请求配置参数 
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("总条数：" + searchResponse.getHits().getTotalHits().value);
        System.out.println("最大得分：" + searchResponse.getHits().getMaxScore());
        // 获取结果
        SearchHit[] hits = searchResponse.getHits().getHits();
        Stream.of(hits).forEach(documentFields -> {
            System.out.println(documentFields.getSourceAsString());
            // 获取高亮字段 
            Map<String, HighlightField> highlightFields = documentFields.getHighlightFields();
            if (highlightFields.containsKey("desc")) {
                // kibana中高亮结果："desc" : ["小浣熊很<span style='color:red'>好吃</span>"]
                // highlightFields.get("desc") 取出高亮结果
                // highlightFields.get("desc").fragments() 高亮结果封装到数组中
                // highlightFields.get("desc").fragments()[0] 取高亮结果的第一个元素 
                System.out.println("desc的高亮结果：" + highlightFields.get("desc").fragments()[0]);
                // 实际生产中，在此处可以取出高亮部分再set进desc字段中。否则查询结果中是无高亮的。看kibana查询结果就知道。
            }
            if (highlightFields.containsKey("title")) {
                System.out.println("title的高亮结果：" + highlightFields.get("title").fragments()[0]);
            }
        });
    }

    /**
     * 上述的查询是query查询。query查询是精确查询，会计算文档的得分。并根据文档的得分进行排序后返回。
     * 
     * filterQuery查询：配合query，在查询中筛选出相关的数据。筛选不会计算文档得分，所以执行效率比query查询快。
     * 先filter过滤查询出数据，再query查询计算得分。filter会对经常使用的filter结果进行缓存。
     * 注意：一旦同时使用query和filterQuery，es优先执行filterQuery然后再执行query
     * 
     * 数据量达到一定级别时，一定要使用filterQuery
     * 
     * 以下查询是先过滤出价格再0~1.5的文档，本次顾虑不计算得分；再在过滤出的文档的基础上进行查询，本次查询计算得分。
     */
    @Test
    public void test14() throws Exception {
        SearchRequest searchRequest = new SearchRequest("product");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder
                // .query(QueryBuilders.matchAllQuery())
                .query(QueryBuilders.termQuery("desc", "浣熊"))
                // 指定过滤条件。此处就是filter 
                // .postFilter(QueryBuilders.termQuery("desc", "好吃"));
                // 对存在某个字段的文档进行过滤
                // .postFilter(QueryBuilders.existsQuery("title"))
                // 过滤出id再1、2、3的文档，在过滤出的文档
                // .postFilter(QueryBuilders.idsQuery().addIds("1", "2", "3"))
                .postFilter(QueryBuilders.rangeQuery("price").gte(0).lte(1.5));
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        // 获取结果
        SearchHit[] hits = searchResponse.getHits().getHits();
        Stream.of(hits).forEach(documentFields -> System.out.println(documentFields.getSourceAsString()));
    }

    /**
     * 聚合查询 
     * 
     * 基于 terms 类型进行聚合。基于字段进行分组聚合
     */
    @Test
    public void test15() throws Exception {
        SearchRequest searchRequest = new SearchRequest("fruit");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 用来设置聚合处理 
        searchSourceBuilder
                // 查询条件。下面是查询所有进行聚合。聚合时不写查询条件默认也是查询所有。
                .query(QueryBuilders.matchAllQuery())
                // 用来设置聚合处理。price_group为自定义的名字 
                .aggregation(AggregationBuilders.terms("price_group").field("price"))
                .size(0);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        // 处理聚合的结果 
        Aggregations aggregations = searchResponse.getAggregations();
        // Aggregation price_group = aggregations.get("price_group");
        ParsedDoubleTerms parsedDoubleTerms = aggregations.get("price_group");
        List<? extends Terms.Bucket> buckets = parsedDoubleTerms.getBuckets();
        buckets.forEach(bucket -> System.out.println(bucket.getKey() + "--" + bucket.getDocCount()));
    }

    /**
     * max=>(ParsedMax) min=>(ParsedMin) avg=>(ParsedAvg) sum=>(ParsedSum) 聚合函数。注意返回的数据中桶里面只有一个返回值 
     */
    @Test
    public void test16() throws Exception {
        SearchRequest searchRequest = new SearchRequest("fruit");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 用来设置聚合处理 
        searchSourceBuilder
                // 查询条件。下面是查询所有进行聚合。聚合时不写查询条件默认也是查询所有。
                .query(QueryBuilders.matchAllQuery())
                // 用来设置聚合处理。 
                // .aggregation(AggregationBuilders.sum("price_sum").field("price"))
                .aggregation(AggregationBuilders.avg("price_avg").field("price"))
                .size(0);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();
        
        // ParsedSum parsedSum = aggregations.get("price_sum");
        // System.out.println(parsedSum.getValue());

        ParsedAvg parsedAvg = aggregations.get("price_avg");
        System.out.println(parsedAvg.getValue());
    }

    private void query(QueryBuilder queryBuilder) throws IOException {
        SearchRequest searchRequest = new SearchRequest("product");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        Stream.of(hits).forEach(documentFields -> System.out.println(documentFields.getSourceAsString()));
    }


}
