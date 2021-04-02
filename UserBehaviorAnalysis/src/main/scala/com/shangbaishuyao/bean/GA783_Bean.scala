package com.shangbaishuyao.bean

import lombok.Data

/**
 * @param pid  用户访问的商品id
 * @param number 用户访问该商品的次数
 */
case class GA783_Bean(
                      pid :String,
                      number: Int
                      )
