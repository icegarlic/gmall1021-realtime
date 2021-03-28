package com.atguigu.gmall1021.publisher.service;

import com.atguigu.gmall1021.publisher.bean.NameValue;

import java.util.List;
import java.util.Map;

public interface OrderAnalysisService {

    List<NameValue> getStateByItem(String itemName,String date, String type);
    Map getDetailByItem(String itemName, String date, Integer pageNo, Integer pageSize);
}
