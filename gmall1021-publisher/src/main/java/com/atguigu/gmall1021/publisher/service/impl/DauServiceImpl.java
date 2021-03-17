package com.atguigu.gmall1021.publisher.service.impl;

import com.atguigu.gmall1021.publisher.service.DauService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;

@Service
public class DauServiceImpl implements DauService {

    @Autowired
    JestClient jestClient;

    final String INDEX_NAME_PREFIX = "gmall1021_dau_info_";

    @Override
    public Long getDauTotal(String date) {
        // 组织查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        // 组织索引名称
        String indexName = INDEX_NAME_PREFIX + date;
        // 构造查询动作
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).build();
        try {
            // 执行查询
            SearchResult result = jestClient.execute(search);
            return result.getTotal(); // 返回查询结果
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("es 查询异常");
        }
    }

    @Override
    public Map<String, String> getDauHourCount(String date) {
        return null;
    }
}
