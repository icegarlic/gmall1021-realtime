package com.atguigu.gmall1021.publisher.service;

import com.atguigu.gmall1021.publisher.bean.NameValue;

import java.util.List;

public interface OrderAnalysisService {

    List<NameValue> getStateByItem(String itemName,String date, String type);
}
