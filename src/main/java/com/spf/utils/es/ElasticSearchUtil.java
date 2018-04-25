package com.spf.utils.es;

import com.alibaba.fastjson.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @author ShuPF
 * @类说明： ES6.2.3 工具类
 * @date 2018-04-20 10:08
 */

public class ElasticSearchUtil {

    private Logger logger = LogManager.getLogger(ElasticSearchUtil.class);

    private static String esIp = "192.168.232.128"; // es服务器ip地址
    private static String esClusterName = "myCluster"; // es cluster_name

    /**
     * 客户端对象
     */
    private TransportClient client;

    /** 索引管理连接 */
    private IndicesAdminClient indicesAdminClient;

    static ElasticSearchUtil instance;

    public static void main(String[] args) {
        ElasticSearchUtil searchUtil = ElasticSearchUtil.getInstance().init();
        searchUtil.getAdminClient();

        //1. 创建索引
        //searchUtil.createIndex("index1");
        //System.out.println("createIndex:"+flag);

        //2. 设置 mappings
        XContentBuilder builder = null;
        try {
            builder = jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("id")
                    .field("type", "long")
                    .endObject()
                    .startObject("title")
                    .field("type", "text")
                    .field("boost", 2)
                    .endObject()
                    .startObject("content")
                    .field("type", "text")
                    .endObject()
                    .startObject("postdate")
                    .field("type", "date")
                    .field("format", "yyyy-MM-dd HH:mm:ss")
                    .endObject()
                    .startObject("url")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject();
            //System.out.println(builder.string());
            //searchUtil.setMapping("index1","novel", builder.string());
        } catch (IOException e) {
            e.printStackTrace();
        }

        Map<String, Object> map = new HashMap<>();
        //map.put("postdate","2018-04-24 16:41:55");
        //map.put("title","斗罗大陆");
        map.put("content","好");
        //map.put("url","http://youku.com");
//        String id = searchUtil.insert("index1","blog",JSONObject.toJSONString(map));
//        System.out.println("id ------ " + id);
        //String data = searchUtil.queryToId("index1","blog","bKeP9WIB7Fk83aqAeMQI");
        //boolean data = searchUtil.updateToId("index1","blog","bKeP9WIB7Fk83aqAeMQI",map);
//        boolean data = searchUtil.deleteToId("index1","blog","bKeP9WIB7Fk83aqAeMQI");
//        System.out.println(data);
//        boolean flag = searchUtil.deleteToId("index1","blog","b6eQ9mIB7Fk83aqALsS5");
//        System.out.println(flag); searchUtil.mustQuery(map,true);
        QueryBuilder queryBuilder = searchUtil.prefixQuery("content","好");
        queryBuilder = QueryBuilders.boolQuery()
                //.must(QueryBuilders.matchPhraseQuery("title","斗破苍穹"))
                //.must(QueryBuilders.matchQuery("content","好"))
                .should(QueryBuilders.matchQuery("title","大陆"));
        List<Map<String, Object>> list = searchUtil.query("index1","blog", queryBuilder,1, 10);
        System.out.println(JSONObject.toJSONString(list));
        searchUtil.close();
    }


    public static ElasticSearchUtil getInstance() {
        synchronized (ElasticSearchUtil.class) {
            if (instance == null) {
                instance = new ElasticSearchUtil();
            }
        }
        return instance;
    }

    public ElasticSearchUtil init (){
        getClient();
        return instance;
    }

    /**
     * 连接集群
     * @return TransportClient 连接
     */
    @SuppressWarnings("resource")
    public TransportClient getClient() {
        if(client==null){
            synchronized (TransportClient.class){
                Settings settings = Settings.builder().put("cluster.name", esClusterName).build();
                try {
                    client=new PreBuiltTransportClient(settings)
                            .addTransportAddress(new TransportAddress(InetAddress.getByName(esIp), 9300));
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                    logger.error("连接 ES 失败", e);
                }
            }
        }
        return client;
    }

    /**
     * 获取索引管理的IndicesAdminClient
     */
    public IndicesAdminClient getAdminClient() {
        if (indicesAdminClient == null) {
            if (client == null) {
                getClient();
            }
            indicesAdminClient = client.admin().indices();
        }
        return  indicesAdminClient;
    }

    /**
     * 判定索引是否存在
     * @param indexName
     * @return
     */
    public boolean isExists(String indexName){
        IndicesExistsResponse response = getAdminClient().prepareExists(indexName).get();
        return response.isExists() ? true : false;
    }
    /**
     * 创建索引
     * @param indexName
     * @return
     */
    public boolean createIndex(String indexName){
        CreateIndexResponse createIndexResponse = getAdminClient()
                .prepareCreate(indexName.toLowerCase())
                .get();
        return createIndexResponse.isAcknowledged() ? true : false;
    }

    /**
     * 创建索引
     * @param indexName 索引名
     * @param shards   分片数
     * @param replicas  副本数
     * @return
     */
    public boolean createIndex(String indexName, int shards, int replicas) {
        Settings settings = Settings.builder()
                .put("index.number_of_shards", shards)
                .put("index.number_of_replicas", replicas)
                .build();
        CreateIndexResponse createIndexResponse = indicesAdminClient
                .prepareCreate(indexName.toLowerCase())
                .setSettings(settings)
                .execute().actionGet();
        return createIndexResponse.isAcknowledged() ? true : false;
    }

    /**
     * 为索引indexName设置mapping
     * @param indexName 索引名
     * @param typeName 类型名
     * @param mapping mapping 内容
     */
    public boolean setMapping(String indexName, String typeName, String mapping) {
        if (!isExists(indexName)) { //索引不存在
            logger.info("esIp:"+esIp+", cluster.name:"+esClusterName+" setMapping Fail ["+indexName+" 索引不存在]");
            return false;
        }

        PutMappingResponse mappingResponse = indicesAdminClient.preparePutMapping(indexName)
                .setType(typeName)
                .setSource(mapping, XContentType.JSON)
                .get();

        System.out.println("mapping toString --- " + mappingResponse.toString());
        return mappingResponse.isAcknowledged() ? true : false;
    }

    /**
     * 删除索引
     * @param indexName
     * @return
     */
    public boolean deleteIndex(String indexName) {
        DeleteIndexResponse deleteResponse = indicesAdminClient
                .prepareDelete(indexName.toLowerCase())
                .execute()
                .actionGet();
        return deleteResponse.isAcknowledged() ? true : false;
    }

    /**
     *  向索引中插入数据
     * @param indexName 索引名
     * @param typeName 类型名
     * @param data 数据
     * @return 插入成功后数据的id值
     */
    public String insert(String indexName, String typeName, String data) {
        if (!isExists(indexName)) { //索引不存在
            logger.info("esIp:"+esIp+", cluster.name:"+esClusterName+" setMapping Fail ["+indexName+" 索引不存在]");
            return null;
        }

        IndexResponse response = this.client.prepareIndex(indexName,typeName)
                                            .setSource(data, XContentType.JSON)
                                            .get();
        return response == null ? null : response.getId();
    }

    /**************************************************** 查询方法 **********************************************************************/

    /**
     *  根据索引ID 获取数据
     * @param indexName 索引名
     * @param typeName 类型名
     * @param id id
     * @return 数据
     */
    public String queryToId(String indexName, String typeName, String id) {
        if (!isExists(indexName)) { //索引不存在
            logger.info("esIp:"+esIp+", cluster.name:"+esClusterName+" setMapping Fail ["+indexName+" 索引不存在]");
            return null;
        }

        GetResponse response = this.client.prepareGet(indexName,typeName,id).get();

        return response == null ? null : JSONObject.toJSONString(response.getSource());
    }

    /**
     * 组合查询
     *      * must(QueryBuilders) :   AND
     *      * mustNot(QueryBuilders): NOT
     *      * should:                  : OR
     *
     * @param indexName 索引名
     * @param typeName 索引类型
     * @param queryBuilder QueryBuilder
     * @param pageNo 起始页
     * @param size 每页显示条数
     * @return list
     */
    public List<Map<String, Object>> query(String indexName, String typeName, QueryBuilder queryBuilder, int pageNo, int size) {
        if (!isExists(indexName)) { //索引不存在
            logger.info("esIp:"+esIp+", cluster.name:"+esClusterName+" setMapping Fail ["+indexName+" 索引不存在]");
            return null;
        }

        int startIndex = pageNo > 0 ? ((pageNo - 1) * size) : 1;

        SearchResponse response = null;
        response = this.client.prepareSearch(indexName)
                                .setTypes(typeName)
                                .setSearchType(SearchType.QUERY_THEN_FETCH)
                                .setQuery(queryBuilder)
                                .setFrom(startIndex)
                                .setSize(size > 0 ? size : 10)
                                .get();

        if (response == null) {
            return null;
        }

        List<Map<String, Object>> list = new ArrayList<>();
        for (SearchHit hit : response.getHits()) {
            list.add(hit.getSourceAsMap());
        }

        return list;
    }

    /**
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * prefix query
     * 包含与查询相匹配的文档指定的前缀。
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    protected QueryBuilder prefixQuery(String name, String content) {
        return QueryBuilders.prefixQuery(name, content);
    }

    /**
     *  所有参数必须匹配的模糊查询，相当于 and 的模糊查询
     * @param params 参数
     * @param isPhrase 是否是文本短语匹配
     * @return
     */
    public QueryBuilder mustQuery(Map<String, Object> params, boolean isPhrase) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        if (params != null) {
            for (String key : params.keySet()) {
                if (isPhrase) {
                    queryBuilder.must(matchPhraseQuery(key,params.get(key)));
                } else {
                    queryBuilder.must(matchQuery(key,params.get(key)));
                }
            }
        }
        return queryBuilder;
    }

    /**
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * match query 单个匹配
     * 根据文本分词匹配
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    protected QueryBuilder matchQuery(String name, Object text) {
        return QueryBuilders.matchQuery(name,text);
    }

    /**
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * match query 单个匹配
     * 根据文本短语匹配
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    protected QueryBuilder matchPhraseQuery(String name, Object text) {
        return QueryBuilders.matchPhraseQuery(name,text);
    }

    /**
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * 匹配多个字段包含的数据 or
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    protected QueryBuilder multiMatchQuery(String text, String ... fiels) {
        return QueryBuilders.multiMatchQuery(text,fiels);             // 设置最小数量的匹配提供了条件。默认为1。
    }

    protected QueryBuilder termsQuery(String name, Object text) {
        return QueryBuilders.termsQuery(name,text);
    }
    /**************************************************** 修改方法 **********************************************************************/

    /**
     *  根据索引 ID 修改数据
     * @param indexName 索引名
     * @param typeName 索引类型
     * @param id 索引id
     * @param data 数据
     * @return 布尔值
     */
    public boolean updateToId(String indexName, String typeName, String id,Map<String, Object> data) {
        if (!isExists(indexName)) { //索引不存在
            logger.info("esIp:"+esIp+", cluster.name:"+esClusterName+" setMapping Fail ["+indexName+" 索引不存在]");
            return false;
        }

        UpdateRequest updateRequest = new UpdateRequest(indexName,typeName,id);

        updateRequest.doc(data);

        UpdateResponse response = null;
        try {
            response = this.client.update(updateRequest).get();
            if (response == null) {
                return false;
            }
        } catch (Exception e) {
            logger.error("{},{},id:{},修改数据失败; data:{}, error:",indexName,typeName,id,JSONObject.toJSON(data), e);
            e.printStackTrace();
        }
        return response.status().getStatus() == 200 ? true : false;
    }

    /**************************************************** 删除方法 **********************************************************************/

    /**
     *  根据索引数据id 删除单条数据
     * @param indexName 索引名
     * @param typeName 索引类型
     * @param id 索引数据id
     * @return 布尔值
     */
    public boolean deleteToId(String indexName, String typeName, String id) {
        if (!isExists(indexName)) { //索引不存在
            logger.info("esIp:"+esIp+", cluster.name:"+esClusterName+" setMapping Fail ["+indexName+" 索引不存在]");
            return false;
        }

        DeleteResponse response = null;
        try {
            response = this.client.prepareDelete(indexName,typeName,id).get();
            if (response == null) {
                return false;
            }
        } catch (Exception e) {
            logger.error("{},{},id:{},删除数据失败; error:",indexName,typeName,id, e);
            e.printStackTrace();
        }

        return response.status().getStatus() == 200 ? true : false;
    }

    /**
     * 关闭连接
     */
    public void close(){
        client.close();
    }
}

