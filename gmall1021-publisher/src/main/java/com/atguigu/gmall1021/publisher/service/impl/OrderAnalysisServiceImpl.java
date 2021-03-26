package com.atguigu.gmall1021.publisher.service.impl;

import com.atguigu.gmall1021.publisher.bean.NameValue;
import com.atguigu.gmall1021.publisher.service.OrderAnalysisService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Service
public class OrderAnalysisServiceImpl implements OrderAnalysisService {

    @Autowired
    JestClient jestClient;

    public static final String INDEX_NAME_PREFIX = "gmall1021_order_wide_";

    @Override
    public List<NameValue> getStateByItem(String itemName, String date, String type) {
        // 查询的索引名
        String indexName = INDEX_NAME_PREFIX + date;
        // 查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 分词查询
        searchSourceBuilder.query(new MatchQueryBuilder("sku_name", itemName).operator(Operator.AND));
        // 聚合
        String groupByField = getGroupbyFieldByType(type);
        TermsAggregationBuilder aggBuilder = AggregationBuilders.terms("groupby_" + groupByField).field(groupByField).size(2);
        searchSourceBuilder.aggregation(aggBuilder);
        //行数为0
        searchSourceBuilder.size(0);
        // 执行查询
        Search search = new Search.Builder(searchSourceBuilder.toString()).build();
        try {
            SearchResult result = jestClient.execute(search);
            // 封装查询结果
            TermsAggregation termsAggregation = result.getAggregations().getTermsAggregation("groupby_" + groupByField);
            List<NameValue> nameValueList = new ArrayList<>();
            if (termsAggregation != null) {
                List<TermsAggregation.Entry> buckets = termsAggregation.getBuckets();
                for (TermsAggregation.Entry bucket : buckets) {
                    String gender = "";
                    if (bucket.getKey().equals("M")) {
                        gender = "男";
                    } else {
                        gender = "女";
                    }
                    // 统计计数
                    BigDecimal count = BigDecimal.valueOf(bucket.getCount());
                    nameValueList.add(new NameValue(gender, count));
                }
            }
            return nameValueList;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询es 异常");
        }
    }

    private String getGroupbyFieldByType(String type) {
        if (type.equals("gender")) {
            return "user_gender";
        } else if (type.equals("age")) {
            return "user_age";
        } else {
            throw new RuntimeException("无此聚合类型");
        }
    }
}
