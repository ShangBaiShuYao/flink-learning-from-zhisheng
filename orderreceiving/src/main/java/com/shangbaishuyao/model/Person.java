package com.shangbaishuyao.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * person
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Person {
    /**
    * id
    */
    private Integer id;

    /**
    * pid
    */
    private String pid;

    /**
    * number
    */
    private String number;
}