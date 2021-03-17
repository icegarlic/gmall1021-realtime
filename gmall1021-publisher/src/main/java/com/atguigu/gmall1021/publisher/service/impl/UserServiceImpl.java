package com.atguigu.gmall1021.publisher.service.impl;

import com.atguigu.gmall1021.publisher.service.UserService;
import org.springframework.stereotype.Service;

@Service
public class UserServiceImpl implements UserService {
    @Override
    public String getUserNameById(String id) {
        if (id.equals("1")){
            return "zhangan";
        } else {
            return "lisi";
        }
    }
}
