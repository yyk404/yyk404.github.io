---
layout: post
title: MapReduce
tags: [MapReduce]
categories: Demo
excerpt_separator: <!--more-->
---

Hadoop-MapReduce与Spark-MapReduce
<!--more-->

## MapReduce

### 体系结构

- Job

  - 一个任务，一个作业
  - 一个Job会被分成多个Task

- Task

  - task里面，又分为Maptask和Reducetask，也就是一个Map任务和Reduce任务

- JobTracker & TaskTracker

  - MapReduce框架采用了Master/Slave架构，包括一个Master和若干个Slave
  - Master上运行JobTracker，负责作业的调度、处理和失败后的恢复
  - Slave上运行的TaskTracker，负责接收JobTracker发给它的作业指令
  
  JobTracker：

  1. 作业调度
  2. 分配任务、监控任务执行进度
  3. 监控TaskTracker的状态
TaskTracker的角色：
  1. 执行任务
  2. 汇报任务状态

MapReduce体系结构主要由四个部分组成，分别是：**Client、JobTracker、TaskTracker以及Task**

Client：客户端，用于提交作业

JobTracker：作业跟踪器，负责作业调度，作业执行，作业失败后恢复

TaskScheduler：任务调度器，负责任务调度

TaskTracker：任务跟踪器，负责任务管理(启动任务，杀死任务等)

### 具体过程

{% include aligner.html images="feature-img/mapreduce-process.png" column=1 %}

#### **Map端**

1. map执行task时, 输入数据来源于HDFS的block, 在MapReduce概念中, map的task只读取split. split与block的对应关系可能是多对一, 默认是一对一.
2. map在写磁盘之前, 会根据最终要传给的reduce把数据划分成相应的分区(partition). 每个分区中,后台线程按键进行排序,如果有combiner,它在排序后的输出上运行.(combiner可以使map的结果更紧凑,减少写磁盘的数据和传递给reduce的数据[省空间和io])
3. map产生文件时, 并不是简单地将它写到磁盘. 它利用缓冲的方式把数据写到内存并处于效率的考虑进行与排序.(如图中 buffer in memory). 每一个map都有一个环形内存缓冲区用于存储任务输出.缓冲区大小默认100MB, 一旦达到阈值(默认80%), 一个后台线程便开始把内容溢出(split)到磁盘.(如果在此期间[split期间]缓冲区被填满,map会被阻塞,直到写磁盘过程完成.
4. 每次内存缓冲区达到阈值移出,就会新建一个溢出文件(split file)(上图 partition,sort and split to disk). 因此**在map任务最后一个记录输出之后,任务完成之前会把一出的多个文件合并成一个已分区且已排序的输出文件.**(上图 merge on task)

#### **Reduce端**

1. map的输出文件在map运行的机器的本地磁盘(reduce一般不写本地), map的输出文件包括多个分区需要的数据, reduce的输入需要集群上多个map的输出. 每个map的完成时间可能不同, 因此只要有一个map任务完成, reduce就开始复制其输出.(上图 fetch阶段) reduce有少量复制线程(默认5个),因此能够并行取得map输出(带宽瓶颈).
2. reduce如何知道从哪台机器获取map结果? map执行完会通知master, reduce总有一个线程定期轮询master(心跳)可以获得map输出的位置. master会等到所有reduce完成之后再通知map删除其输出.
3. 如果map的输出很小,会被复制到reduce任务jvm的内存.否则map输出会被复制到磁盘(又写磁盘)
4. 复制完所有map输出后,reduce任务进入排序合并阶段(其实是合并阶段,因为map的输出上有序的).这个维持顺序的合并过程是循环进行的.(比如50个map输出,合并因子是10(默认值), 则合并将进行5次, 每次合并10个文件, 最终有5个中间文件)
5. 在最后reduce阶段,直接把数据输入reduce函数(上面的5个中间文件不会再合并成一个已排序的中间文件). 输出直接写到文件系统, 一般为HDFS.



**MR的shuffle分为：**

1. Map端的shuffle，主要是Partition、Collector、Sort、Spill、Merge几个阶段；
2. Reduce端的shuffle，主要是Copy、Merge、Reduce几个阶段。

但是MR的shuffle有一个很重要的特点：**全局排序**。

MR的shuffle过程中在Map端会进行一个Sort，也会在Reduce端对Map的结果在进行一次排序。这样子最后就变成了有多个溢出文件（单个溢出文件是有序的，但是整体上是无序的），那么最后在merge成一个输出文件时还需再排序一次，同时，reduce在进行merge的时候同样需要再次排序（因为它从多个map处拉数据）

> 注意：这个有序是指Key值有序，对于value依旧是无序的，如果想对value进行排序，需要借鉴二次排序的算法。
> 二次排序的理论是利用MR的全局排序的功能，将value和key值合并，作为一个新的Key值，然后由MR的机制进行Key的排序，这个方法类似于在处理数据倾斜的时候在Key值上加随机数的方法。
>
> 

## **Spark的shuffle（对排序和合并进行了优化）：**

为了避免不必要的排序，Spark提供了基于Hash的、基于排序和自定义的shuffleManager操作。

Spark在DAG阶段以宽依赖shuffle为界，划分stage，上游stage做map task，每个map task将计算结果数据分成多份，每一份对应到下游stage的每个partition中，并将其临时写到磁盘，该过程叫做shuffle write。

下游stage做reduce task，每个reduce task通过网络拉取上游stage中所有map task的指定分区结果数据，该过程叫做shuffle read，最后完成reduce的业务逻辑。



|                         | Hadoop Shuffle                                               | Spark Shuffle                                                |
| ----------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 逻辑流                  | map()，spill，merge，shuffle，sort，reduce()等，是按照流程顺次执行的，属于Push类型 | 由算子进行驱动，由于Spark的算子懒加载特性，属于Pull类型，整个Shuffle过程可以划分为Shuffle Write 和Shuffle Read两个阶段； |
| 数据结构                | 基于文件的数据结构                                           | 基于RDD的数据结构，计算性能要比Hadoop要高                    |
| Map过程产生中间文件数量 | 在**环形缓冲区溢出到磁盘**很多小文件，和map task及key hash分区的数量有关，不过可以采用combiner的机制对文件进行合并； | 与ShuffleManager的类型有关                                   |



