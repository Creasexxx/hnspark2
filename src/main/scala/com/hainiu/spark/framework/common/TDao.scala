package com.hainiu.spark.framework.common

import com.hainiu.spark.framework.util.EnvUtil

trait TDao {

    def readFile(path:String) = {
        EnvUtil.take().textFile(path)
    }
}
