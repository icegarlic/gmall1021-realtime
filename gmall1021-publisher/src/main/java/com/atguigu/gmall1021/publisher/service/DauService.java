package com.atguigu.gmall1021.publisher.service;

import java.util.Map;

public interface DauService {
    /**
     * 根据日期查寻总数
     * @param date 日期
     * @return 总数
     */
    Long getDauTotal(String date);

    /**
     * 根据日期查询当日分时明细
     * @param date 日期
     * @return 明细 Map
     */
    Map<String,String> getDauHourCount(String date);
}
