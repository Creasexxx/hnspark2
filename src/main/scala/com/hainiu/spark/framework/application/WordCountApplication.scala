package com.hainiu.spark.framework.application

import com.hainiu.spark.framework.common.TApplication
import com.hainiu.spark.framework.controller.WordCountController

object WordCountApplication extends App with TApplication{

    // 启动应用程序
    start(){
        val controller = new WordCountController()
        controller.dispatch()
    }

}
