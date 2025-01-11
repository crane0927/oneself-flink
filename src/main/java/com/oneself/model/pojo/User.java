package com.oneself.model.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * @author liuhuan
 * date 2025/1/8
 * packageName com.oneself.model.pojo
 * className User
 * description
 * 1. 类是 public
 * 2. 有一个无参构造
 * 3. 所有属性都是 public 的
 * 4. 所有属性的类型都是可以序列化的
 * Flink 会把这样的类作为一种特殊的 POJO（Plain Ordinary Java Object）数据类型来对待，方便数据的解析和序列化
 * version 1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true) // 忽略未知字段
public class User implements Serializable {

    private static final long serialVersionUID = 1L;
    public Integer id;
    public String name;
    public Integer age;

    public User() {
    }

    public User(Integer id, String name, Integer age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
}
