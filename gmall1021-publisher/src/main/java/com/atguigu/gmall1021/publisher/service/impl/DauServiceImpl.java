package com.atguigu.gmall1021.publisher.service.impl;

import com.atguigu.gmall1021.publisher.service.DauService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DauServiceImpl implements DauService {

    @Autowired
    JestClient jestClient;

    final String INDEX_NAME_PREFIX = "gmall1021_dau_info_";

    /*
        查询3件事 1 构造查询条件 2 提交到数据库 3 提取查询结果
     */
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
    public Map<String, Long> getDauHourCount(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);

        // 组合聚合语句
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("groupby_hr").field("hr").size(24);
        searchSourceBuilder.aggregation(aggregationBuilder);

        // 组织索引名称
        String indexName = INDEX_NAME_PREFIX + date;
        // 构造查询动作
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).build();
        try {
            // 执行查询
            SearchResult result = jestClient.execute(search);
            // 提取聚合结果
            TermsAggregation termsAggregation = result.getAggregations().getTermsAggregation("groupby_hr");
            if (termsAggregation != null) {
                List<TermsAggregation.Entry> buckets = termsAggregation.getBuckets();
                Map<String, Long> hourCountMap = new HashMap<>();
                for (TermsAggregation.Entry bucket : buckets) {
                    hourCountMap.put(bucket.getKey(), bucket.getCount());
                }
                return hourCountMap;
            } else {
                return new HashMap<>();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("es 查询异常");
        }

    }
}
