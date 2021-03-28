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
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class OrderAnalysisServiceImpl implements OrderAnalysisService {

    @Autowired
    JestClient jestClient;

    public static final String INDEX_NAME_PREFIX = "gmall1021_order_wide_";

    @Override
    public List<NameValue> getStateByItem(String itemName, String date, String type) {

        //查询的索引名
        String indexName = INDEX_NAME_PREFIX + date;
        //查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 分词搜索
        searchSourceBuilder.query(new MatchQueryBuilder("sku_name", itemName).operator(Operator.AND));
        //聚合
        String groupByField = getGroupbyFieldByType(type);
        TermsAggregationBuilder aggBuilder = AggregationBuilders.terms("groupby_" + groupByField).field(groupByField).size(2);
        searchSourceBuilder.aggregation(aggBuilder);
        //行数为0
        searchSourceBuilder.size(0);
        //执行查询
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).build();
        try {
            SearchResult result = jestClient.execute(search);
            //封装查询结果
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
                    //统计计数
                    BigDecimal count = BigDecimal.valueOf(bucket.getCount());
                    nameValueList.add(new NameValue(gender, count));

                }

            }
            return nameValueList;

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES 异常");
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

    @Override
    public Map getDetailByItem(String itemName, String date, Integer pageNo, Integer pageSize) {
        //索引名称
        String indexName = INDEX_NAME_PREFIX + date;
        //组合查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 分词搜索
        searchSourceBuilder.query(new MatchQueryBuilder("sku_name", itemName).operator(Operator.AND));
        //分页 //from 行号= (页码-1）*每页行数
        searchSourceBuilder.from((pageNo - 1) * pageSize);
        searchSourceBuilder.size(pageSize);
        //高亮
        searchSourceBuilder.highlighter(new HighlightBuilder().field("sku_name"));
        //查询列
        searchSourceBuilder.fetchSource(new String[]{"sku_name", "create_time", "order_price", "province_name", "sku_num", "total_amount", "user_age", "user_gender"}, null);

        //执行查询
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            //提取总数
            Long total = searchResult.getTotal();
            //提取明细    1：男女文字转换  2 使用高亮的名称字段
            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            List<Map> detailList = new ArrayList<>();
            for (SearchResult.Hit<Map, Void> hit : hits) {
                Map detailMap = hit.source;
                //取高亮值替换原值
                String skuNameHL = hit.highlight.get("sku_name").get(0);
                detailMap.put("sku_name", skuNameHL);
                //男女文字转换
                if ("M".equals(detailMap.get("user_gender"))) {
                    detailMap.put("user_gender", "男");
                } else {
                    detailMap.put("user_gender", "女");
                }
                detailList.add(detailMap);
            }
            Map resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("detail", detailList);
            return resultMap;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES 异常");
        }

        //处理返回结果

    }
}
