package cn.king.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


/**
 * @author: wjl@king.cn
 * @time: 2022/7/31 15:54
 * @version: 1.0.0
 * @description:
 */
@Configuration
public class ESConfig {

    @Value("${elasticsearch.host}")
    private String host;

    @Bean
    public RestHighLevelClient restHighLevelClient() {
        return new RestHighLevelClient(RestClient.builder(new HttpHost("txyun", 9200, "http")));
    }

}
