package com.atguigu.gmall1021.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall1021.publisher.service.DauService;
import com.atguigu.gmall1021.publisher.service.UserService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    UserService userService;

    @Autowired
    DauService dauService;

    @RequestMapping("/hello")
    public String helloWorld(@RequestParam("name") String name, @RequestParam("age") int userAge) {
        return "hello " + name + "年龄：" + userAge;
    }

    @RequestMapping("/user")
    public String getUserName(@RequestParam("id") String id) {
        String name = userService.getUserNameById(id);
        return name;
    }

    @RequestMapping("/dauRealtime")
    public String getDauRealtime(@RequestParam("td") String td) {
        Long dauTotal=dauService.getDauTotal(td); //通过 td 从es中查询
        if(dauTotal==null){
            dauTotal=0L;
        }
        Map dauTdMap=dauService.getDauHourCount(td);//通过 td  从es中查询
        //时间减一天
        String yd = getYd(td);
        Map dauYdMap=dauService.getDauHourCount(yd);//通过 td-1天  从es中查询

        Map resultMap=new HashMap();
        resultMap.put("dauTotal",dauTotal);
        resultMap.put("dauTd",dauTdMap);
        resultMap.put("dauYd",dauYdMap);
        return JSON.toJSONString(resultMap);
    }

    private String getYd(String td) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date todyDate = dateFormat.parse(td);
            Date yesterdayDate = DateUtils.addDays(todyDate, -1);
            return dateFormat.format(yesterdayDate);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException("转换日期异常");
        }
    }
}
