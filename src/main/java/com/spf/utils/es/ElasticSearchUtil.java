package com.spf.utils.es;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.XSlf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @author ShuPF
 * @类说明： ES6.2.3 工具类
 * @date 2018-04-20 10:08
 */

public class ElasticSearchUtil {

    private Logger logger = LogManager.getLogger(ElasticSearchUtil.class);

    public static final int SEARCH_FROM = 0; // 起始位置
    public static final int SEARCH_SIZE = 30; // 搜索记录数
    private static String esIp = "192.168.232.128"; // es服务器ip地址
    private static String esPort = "9200"; // es服务端口
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
        map.put("postdate","2018-04-20 17:41:55");
        map.put("title","斗罗大陆");
        map.put("content","很牛逼");
        map.put("url","http://iyiqi.com");
        //String id = searchUtil.insert("index1","blog",JSONObject.toJSONString(map));
        //System.out.println("id ------ " + id);
        String data = searchUtil.queryToId("index1","blog","09Nu4mIBGA5fd7IVvDSN");
        System.out.println(data);
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
     * 关闭连接
     */
    public void close(){
        client.close();
    }
}

