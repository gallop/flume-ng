/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.elasticsearch.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.IndexNameBuilder;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.*;

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_PORT;

/**
 * Rest ElasticSearch client which is responsible for sending bulks of events to
 * ElasticSearch using ElasticSearch HTTP API. This is configurable, so any
 * config params required should be taken through this.
 */
public class  ElasticSearchRestClient implements ElasticSearchClient {

  public static final Charset charset = Charset.defaultCharset();
  private static final String INDEX_OPERATION_NAME = "index";
  private static final String INDEX_PARAM = "_index";
  private static final String TYPE_PARAM = "_type";
  private static final String TTL_PARAM = "_ttl";
  private static final String BULK_ENDPOINT = "_bulk";

  private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestClient.class);

  private final ElasticSearchEventSerializer serializer;
  private final HttpHost[] hosts;
  
  //private StringBuilder bulkBuilder;
 //private HttpClient httpClient;

  private RestHighLevelClient client;
  private final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
  private BulkRequest bulkRequest;
  private IndexRequest indexRequest;

  
  public ElasticSearchRestClient(String[] hostNames,
      ElasticSearchEventSerializer serializer) {
    /*for (int i = 0; i < hostNames.length; ++i) {
      if (!hostNames[i].contains("http://") && !hostNames[i].contains("https://")) {
        hostNames[i] = "http://" + hostNames[i];
      }
    }*/
    this.serializer = serializer;
    hosts = configureHostnames(hostNames);
    /*credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "123456"));*/
    client = new RestHighLevelClient(
            RestClient.builder(hosts)
                    .setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                      //这里可以设置一些参数，比如cookie存储、代理等等
                      httpAsyncClientBuilder.disableAuthCaching();
                      return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    })
    );

  }


  @Override
  public void configure(Context context) {
  }

  @Override
  public void close() {
    if (client != null) {
      try {
        client.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    client = null;

  }

  @Override
  public void addEvent(Event event, IndexNameBuilder indexNameBuilder, String indexType,
                       long ttlMs) throws Exception {
    //1：构建bulk批量操作
    if (bulkRequest == null) {
      bulkRequest = new BulkRequest();
    }
    XContentBuilder builder = serializer.getContentBuilder(event);
    String jsonStr = new String(event.getBody(), charset);
    System.err.println("------indexType:"+indexType);
    indexRequest = new IndexRequest(indexNameBuilder.getIndexName(event));//.source(builder)
    //indexRequest.source(jsonStr, XContentType.JSON);
    indexRequest.source(builder);
    //2：通过add操作实现一个请求执行多个操作
    synchronized (bulkRequest){
      bulkRequest.add(indexRequest);
    }

  }

  @Override
  public void execute() throws Exception {
    /*try {
      IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
      logger.info("gallop---response:"+response.toString());
    } catch(ElasticsearchException e) {
      logger.error(e.getMessage());
      System.err.println("===========rest-client-execute-err=====================");
      e.printStackTrace();
    }*/
    synchronized (bulkRequest) {
      try {
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        RestStatus status = bulk.status();
        System.out.println("gallop---response:"+status);
      } catch (IOException e) {
        logger.error(e.getMessage());
        System.err.println("===========rest-client-execute-err=====================");
        e.printStackTrace();
      }
      bulkRequest = new BulkRequest();
    }


  }

  private HttpHost[] configureHostnames(String[] hostNames) {
    logger.warn(Arrays.toString(hostNames));

    //serverAddresses = new TransportAddress[hostNames.length];
    List<HttpHost> hostsList = new ArrayList<>(hostNames.length);
    for (int i = 0; i < hostNames.length; i++) {
      String[] hostPort = hostNames[i].trim().split(":");
      String host = hostPort[0].trim();
      int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1].trim())
              : DEFAULT_PORT;
      hostsList.add(new HttpHost(host, port, "http"));
    }
    HttpHost[] hosts = new HttpHost[hostsList.size()];
    hostsList.toArray(hosts);
    return hosts;
  }
}
