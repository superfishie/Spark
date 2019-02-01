# Spark完整使用案例

## 一. 代码示例
先来看一个spark小程序来感受一下:     
程序名称: first_test.py          
程序绝对路径: /home/zmate/PycharmProjects/ZM_Lab_util/spark_architecture      
```python
from pyspark import SparkConf, SparkContext
import time


def f(x):
    result = x * x
    for i in range(1, 50):  
        result += (x + i) * (x + i)
    if x % 1000 == 0:
        print x
    return result


def run(sc):
    start_time = time.time()
    # 创建一个并行集合(产生10^7个参数),将数据集切分为50份
    rdd = sc.parallelize(range(1000 * 10000), 50)
    # 将RDD中的每个数据项，通过map中的函数映射变为一个新的元素
    rdd = rdd.map(f)
    print rdd.count()

    print 'time consume:', time.time() - start_time


if __name__ == "__main__":
    conf = SparkConf().setAppName('performance')
    sc = SparkContext(conf=conf)
    run(sc)
    sc.stop()
```
$f(x) = x^2 + (x+1)^2 + ... + (x+49)^2$ , 如果要求x在0 ~ $10^7$范围内的函数值,由于数据量过大,
我们可以将$x$的数据分发到多台机器上进行计算,此时spark就可以发挥作用了. 由主节点(master)产生数据或参数,分发到各个子节点(slave).     

使该程序在spark上运行只需两步:        
1. 打开终端,进入到first-test.py的文件目录

2. 在终端中输入`/home/hadoop/spark/bin/spark-submit --master spark://liuqiang-pc:7077 first_test.py`  

其中, `/home/hadoop/spark/bin/spark-submit`是公司提交spark文件的目录,公司每一台机器的提交文件目录都是一致的; `spark://liuqiang-pc:7077`是主节点地址,spark程序需要提交到主节点运行.

程序运行截图如下:
![avatar](https://github.com/superfishie/img_host/blob/master/Spark_first_test.png?raw=true)
上述程序如果可以正常运行,说明本机的spark已可以正常使用了.

## 二. 代码结构
根据spark的运行流程,创建三个类: RunSpark类,TaskGenerator类和worker类.

### 2.1 TaskGenerator
| name | TaskGenerator |  
|------|:-------------:|  
| 功能  | 生成任务列表, 一般是列表里包含着许多字典数据 |
| 参数  | cfg   |
| 属性  | cfg, n, param ...|
| 方法  | generate_param_list |

| 参数名称 | 参数说明        | 数据类型|
|:------: |:-------------:|:------:|
| cfg     | 所有试验的配置总参数,包括时间跨度,模型名称,模型参数,抽样次数等| dict|

 对象属性一般有时间跨度,模型名称,模型参数,抽样次数等,但不是必须包括的,需要根据情况具体分析.  

| 属性名称 | 属性说明        | 数据类型|
|:------: |:-------------:|:------:|
| cfg     | 所有试验的配置总参数,包括时间跨度,模型名称,模型参数,抽样次数等| dict|
| t       | 时间跨度,一般有7day,14day,30day,60day | dict|
| n_sample| 抽样次数| int|
| param | 模型参数,机器学习中配置模型的参数| dict |

generate_param_list实现生成任务参数的功能  

| 方法名称| 功能说明        |
|:------:|:-------------:|
| generate_param_list | 通过传入的参数返回包含字典的列表,每个字典即是单个任务包含的参数|

**示例代码:**
```python
class TaskGenerator(object):
    def __init__(self, cfg):
        self.cfg = cfg
        self.t = cfg.get("t")
        self.n = cfg.get("n_sample")
        self.factor_num = cfg.get("factor_num")
        self.model_name = cfg.get("model_name")
        self.tag_num = cfg.get("tag_num")
        self.param = cfg.get("param")

    def generate_param_list(self, total_data_start="2017-11-01T00-00-00Z", total_data_end="2018-08-31T23-59-00Z"):

        total_start = ToolDateTime.string_to_int(total_data_start)
        total_end = ToolDateTime.string_to_int(total_data_end)

        param_list = []
        for k, v in self.t.items():     # k: 30day v:[43200, 0.2]
            step = int(v[0] / 2000)
            # 在固定的时间段内抽样self.param_n次
            n_sample_start_int_list = np.random.randint(total_start, total_end - 2*v[0] * 60 * 10 ** 6, size=self.n)

            for start_int in n_sample_start_int_list:
                start_time_t = ToolDateTime.int_to_string(start_int, 'min')
                end_time_t = ToolDateTime.int_to_string(start_int + v[0] * 60 * 10 ** 6, 'min')
                start_time_p = ToolDateTime.int_to_string(start_int + (v[0]+1) * 60 * 10 ** 6, 'min')
                end_time_p = ToolDateTime.int_to_string(start_int + 2*v[0] * 60 * 10 ** 6, 'min')
                n = np.argwhere(n_sample_start_int_list == start_int)[0][0]

                param_dict = {"start_time_t": start_time_t,
                              "end_time_t": end_time_t,
                              "start_time_p": start_time_p,
                              "end_time_p": end_time_p,
                              "step": step,
                              "id": {"t": k, "n_sample": n},
                              "factor_num": self.factor_num,
                              "tag_num": self.tag_num,
                              "model_name": self.model_name,
                              "param": self.param
                              }
                param_list.append(param_dict)

        return param_list

  if __name__ == '__main__':


      cfg = {"model_name": "decision_tree_classifier",
                    "param":{},
                    "t": {"7day": [7 * 1440, 0.2],
                          "14day": [14 * 1440, 0.2],
                          "30day": [30 * 1440, 0.2],
                          "60day": [60 * 1440, 0.2],
                          "90day": [90 * 1440, 0.2]},
                    "n_sample": 20,
                    "factor_num": 1205,
                    "tag_num":294
                  }

      g = TaskGenerator(cfg)
      parm_list = g.generate_param_list()
      print parm_list

```

### 2.2 Worker(任务处理类)
在生成了任务列表之后,将任务列表中的每一个元素作为参数,传入任务处理类中的任务处理方法进行集中处理. 任务处理方法可以是静态函数,也可以是普通方法或静态方法. **唯一的要求是:任务处理函数的参数只有一个且参数类型只能是dict**

| 参数名称 | 参数说明        | 数据类型|
|:------: |:-------------:|:------:|
| param_dict| 包含单个任务参数的字典 | dict|

**示例代码: 以普通方法Dp.run_dp为例**
```python
Eg:
param_dict = {"start_time_t": "2017-11-03T00-00-00Z",
              "end_time_t": "2017-12-03T00-00-00Z",
              "start_time_p": "2018-01-03T00-00-00Z",
              "end_time_p": "2017-02-03T00-00-00Z",
              "step": 21,
              "id": {"t": "30day", "n_sample": 2},
              "factor_num": 1205,
              "model_name":"hmm" ,
              "param": {"n_components": 3}}


class DP6:
  def __init__():
    pass

   .....

  def run_dp(self,parm_dict):

    # 获取参数
    start_time_t = parm_dict.get("start_time_t")
    end_time_t = parm_dict.get("end_time_t")
    start_time_p = parm_dict.get("start_time_p")
    end_time_p = parm_dict.get("end_time_p")
    factor_num = parm_dict.get("factor_num")
    task_id = parm_dict.get("id")
    step = parm_dict.get("step")
    param = parm_dict.get("param")


    # 通过参数获取数据
    X_train, X_test = self.import_data(start_time_t=start_time_t, end_time_t=end_time_t,
                                       start_time_p=start_time_p, end_time_p=end_time_p,
                                       step=step, factor_num=factor_num)


    # 批量训练模型,返回模型
    model = self.train_model(X_train, param)

    # 批量预测,返回predict_y的DataFrame
    y_p = self.predict_model(X_test, model)

    return y_p

```

### 2.3 RunSpark
有了任务列表和任务处理类之后,我们就可以生成RunSpark部署Spark了.

| name | RunSpark |  
|------|:-------------:|  
| 功能  |结合TaskGenerator和任务处理函数运行spark|
| 参数  | cfg  |
| 属性  | cfg, app_name, num_slices, ...|
| 方法  | run_spark_with_data,run |


| 参数名称 | 参数说明        | 数据类型|
|:------: |:-------------:|:------:|
| cfg     | 所有试验的配置总参数,包括时间跨度,模型名称,模型参数,抽样次数等| dict|


| 属性名称 | 属性说明        | 数据类型|
|:------: |:-------------:|:------:|
| cfg     | 所有试验的配置总参数,包括时间跨度,模型名称,模型参数,抽样次数等| dict|
| app_name | 此次spark任务的名称| string|
| num_slices| 任务的切分次数| int|


run_spark_with_data 将任务列表中的每一组参数代入任务处理函数中,让spark进行并行计算.
| 方法名称| 功能说明        |
|:------:|:-------------:|
| run_spark_with_data | 生成RDD把任务列表的参数代入处理函数进行分布式计算|
| run | 传入相应参数运行run_spark_with_data|

一般情况下,在run_spark_with_data有两种读取数据的方式:  

**第一种: worker类中的任务处理函数具有数据库交互方法使所有子机器自行完成数据库存取.**
**示例代码:**
```python
from pyspark import SparkConf, SparkContext
from datetime import datetime
from development.alpha_production.dp.dp6_spark import DP6
from development.alpha_production.dp.dp6_spark import TaskGenerator


class RunSpark(object):
    def __init__(self, func, **kwargs):
        self.cfg = kwargs
        self.app_name = "spark_test_dp6"
        self.num_slices = 50
        self.func = func

    def run_spark_with_data(self, app_name, num_slices,func):
        conf = SparkConf().setAppName(app_name)
        sc = SparkContext(conf=conf)
        sc.addFile(self.conf_path) #添加配置文件的路径
        output_info = app_name + "application start:" + datetime.now().isoformat()
        print(output_info)

        tg = TaskGenerator(self.cfg)
        param_list = tg.generate_param_list()
        rdd = sc.parallelize(param_list, num_slices)
        rdd = rdd.map(func)
        result_list = rdd.collect()

        # 处理结果
        print(result_list)

        sc.stop()

    def run(self):
        self.run_spark_with_data(app_name=self.app_name, num_slices=self.num_slices, func=self.func)


if __name__ == '__main__':

    proj_path = os.path.split(os.path.abspath(os.path.realpath(__file__)))[0] + "/../../../../"

    # conf_path是配置文件的路径
    cfg = {
        "conf_path": os.path.abspath(os.path.join(proj_path, "configure/db_conf/mongo_211_ZMingStopLose.conf")),
        "collection": {
            "source_collection": "alpha_af_pool_btc_usd",
            "tag_collection": "alpha_rf_pool_btc_usd",
            "target_collection": "alpha_ic_pool_single_btc_usd"
        },
        "param": {
            "t": {
                "7day": [7 * 1440, 0.2],
                "14day": [14 * 1440, 0.2],
                "30day": [30 * 1440, 0.2],
                "60day": [60 * 1440, 0.2],
                "90day": [90 * 1440, 0.2]},
            "n_sample": 1,
            "RF_param": {
                "RF2_60min": [60, 0.2],
                "RF2_180min": [180, 0.2],
                "RF2_360min": [360, 0.2],
                "RF2_720min": [720, 0.2],
                "RF2_1440min": [1440, 0.2]},
            "m": {
                "IC": 0.3,
                "DC": 0.3,
                "MIC": 0.4
            },
            'q_list': [0.5, 0.75, 0.9, 0.95]
        },
    }

    run = RunSpark(DP6("btc_usd").run_dp, **cfg)
    run.run()
```
**第二种: 通过worker类一次性把数据从读出,分发给子机器,worker类中的任务处理函数只管处理数据无须与数据库交互 .**
**示例代码:**

worker.py
worker类,通过get_all_corrs()将数据一次性全部读出,保存到self.data_dict中.

```python
class FactorScore:
    """
    计算数据库中因子相关系数得分值,生成得分表
    """

    def __init__(self, collection_IC):
        self.collection_IC = collection_IC
        self.data_dict = {}
        self.factors_name_list = []

    def get_all_corrs_name(self, collection_name='alpha_af_pool_btc_usd'):
        """
        获取各IC的名称列表
        :return:
        """
        stop_lose_mongo_client = DbOperation.get_mongo_client()
        RF_filter = {"_id": "2018-01-01T00-00-00Z"}
        records = stop_lose_mongo_client.find(collection_name=collection_name, filter=RF_filter)
        for record in records:
            for key in record.keys():
                self.factors_name_list.append(key)
        self.factors_name_list.remove('_id')
        return self.factors_name_list

    def get_all_corrs(self):
        # 将数据库中的相关系数读入内存中,并构建成一个大字典
        stop_lose_mongo_client = DbOperation.get_mongo_client()
        records = stop_lose_mongo_client.find(collection_name=self.collection_IC)

        for record in records:
            self.data_dict[record['_id']] = record['value']
        return self.data_dict

    # 任务处理函数
    def get_one_score_sheet(self, param_dict):
        pass
```
RunSpark.py
在主节点中调用 DC_score.get_all_corrs() 获取了全部的数据,再通过rdd = rdd.map(DC_score.get_one_score_sheet) 将数据分发到各个子机器.
```python
class RunSpark(object):
    def __init__(self, **kwargs):
        self.cfg = kwargs
        self.conf_path = kwargs.get("conf_path")
        self.collection = kwargs.get("collection")
        self.target_collection = self.collection.get("target_collection")
        self.source_collection = self.collection.get("source_colleciton")
        self.app_name = "spark_CalcScore"
        self.num_slices = 100

    def run_spark_with_data(self, app_name, num_slices):
        conf = SparkConf().setAppName(app_name)
        sc = SparkContext(conf=conf)
        sc.addFile(self.conf_path)
        output_info = app_name + "application start:" + datetime.now().isoformat()
        print(output_info)

        DC_score = FactorScore(self.target_collection)
        DC_score.get_all_corrs()  # 获得数据库中所有的相关系数数据
        DC_score.get_all_corrs_name()  # 获得因子名称列表

        tg = TaskGenerator(self.cfg)
        param_list = tg.generate_param_list()
        rdd = sc.parallelize(param_list, num_slices)
        rdd = rdd.map(DC_score.get_one_score_sheet)
        result_list = rdd.collect()

        # 处理结果,保存文件到本地
        final_score_sheet = sum(result_list)
        final_score_sheet.to_csv('scores.csv')

        sc.stop()
        return final_score_sheet

    def run(self):
        return self.run_spark_with_data(app_name=self.app_name, num_slices=self.num_slices)

```
将数据库中的数据一次全部读出,存入内存,再在RunSpark里分发给slave机器,这种方法适用于数据不太大的情况.
### 2.4 运行Spark
1.打开终端,进入项目目录 OR 进入项目目录再右键打开终端.
![avatar](https://github.com/superfishie/img_host/blob/master/Spark_open_terminator.png?raw=true)      
2.Ctrl+A 将项目目录下的文件全部打包成.zip文件,注意不是将项目文件夹打包而是将项目文件夹下的文件夹打包
![avatar](https://github.com/superfishie/img_host/blob/master/Spark_zip_files.png?raw=true)      
3.在终端中输入: ```/home/hadoop/spark/bin/spark-submit --master spark://liuqiang-pc:7077 --py-files ZM_Lab_Alpha_System.zip development/alpha_production/dp/run_spark_dp6.py```
![avatar](https://github.com/superfishie/img_host/blob/master/Spark_run.png?raw=true)  
```/home/hadoop/spark/bin/spark-submit```: 运行spark程序的入口    
```spark://liuqiang-pc:7077```: 集群主节点     
```--py-files ZM_Lab_Alpha_System.zip``` : spark程序所有依赖的文件打包
```development/alpha_production/dp/run_spark_dp6.py``` spark程序的存放路径

所有,启动spark程序的通用格式是:    
spark运行程序入口  --master spark集群主节点 --spark相关文件.zip spark程序

## 三.注意事项
 ### 3.1 重启spark服务
 通常服务处于启动状态，不需要我们启动或关闭。web展示：http://192.168.1.139:8080/
如果确实需要重启spark服务，命令如下。
```
进入主节点
ssh hadoop@192.168.1.139
关闭服务
hadoop@liuqiang-pc:~/spark/sbin$ ./stop-all.sh
启动服务
hadoop@liuqiang-pc:~/spark/sbin$ ./start-all.sh
```

### 3.2 关于依赖的问题
用spark调试程序时,往往会遇到可以单机运行但不能spark运行的情况. 这有可能是slave机没有找到相关的依赖.

1. 使用者需要理解的是，使用spark运行Python代码时，不论这行代码是运行在master节点或是slave节点，他寻找一切依赖的自定义模块时，都是从zip中寻找。

2. 在启动Spark时,如果需要添加依赖, “--py-files zip的路径” 与 “--files 配置文件的路径” 均可在代码中使用 sc.addFile() 和 sc.addPyFile() 替代.也可以看出py格式的文件和非py格式的文件在添加时的写法是不一样的.

2. 若zip中存在需要导入的非py格式的配置文件,需要依照以下方法导入,否则会报错.因为py     
外部配置文件不能直接被找到,需要在命令行中用 “--files 配置文件的路径” ,或在代码中使用 SparkContext.addFile()分发到到slave机,每台slave机再用SparkFiles.get(file_name)找到配置文件.
E.g.
如果需要在 slave 机器中读取数据库，推荐使用此种方法启动。
    ```python
    from pyspark import SparkFiles
    file_conf = SparkFiles.get("mongo_211_ZMingStopLose.conf")
    ```


**注意:不要在worker类中的__init__函数中初始化MongoClient对象,否则会出现如下错误:**
```
pickle.PicklingError: Could not serialize object: TypeError: can't pickle thread.lock objects
```


## 四. Spark运行流程:
查看slave的日志可知slave的运行流程:  

**1. 初始化本机(192.168.1.83)的spark:**
* 连接主节点-- driver:192.168.1.122:42737;
* 在本地分配内存366.3 MB
* worker spark连接本地端口192.168.1.83:38429 并在driver(主节点上注册)
* 在本地端口192.168.1.83:35521 开启spark服务并注册BlockMananger
* slave获得3个任务
```
19/01/31 14:52:29 INFO TransportClientFactory: Successfully created connection to /192.168.1.122:42737 after 1 ms (0 ms spent in bootstraps)
19/01/31 14:52:29 INFO DiskBlockManager: Created local directory at /tmp/spark-00ad7881-80c2-451f-9573-1e52478e74f5/executor-c6674c9e-c231-4e70-a739-2bf8cf5da17e/blockmgr-59405f39-1cf6-4c25-952c-e1084f780f34
19/01/31 14:52:29 INFO MemoryStore: MemoryStore started with capacity 366.3 MB
19/01/31 14:52:29 INFO CoarseGrainedExecutorBackend: Connecting to driver: spark://CoarseGrainedScheduler@192.168.1.122:42737
19/01/31 14:52:29 INFO WorkerWatcher: Connecting to worker spark://Worker@192.168.1.83:38429
19/01/31 14:52:29 INFO TransportClientFactory: Successfully created connection to /192.168.1.83:38429 after 1 ms (0 ms spent in bootstraps)
19/01/31 14:52:29 INFO WorkerWatcher: Successfully connected to spark://Worker@192.168.1.83:38429
19/01/31 14:52:29 INFO CoarseGrainedExecutorBackend: Successfully registered with driver
19/01/31 14:52:29 INFO Executor: Starting executor ID 2 on host 192.168.1.83
19/01/31 14:52:30 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 35521.
19/01/31 14:52:30 INFO NettyBlockTransferService: Server created on 192.168.1.83:35521
19/01/31 14:52:30 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
19/01/31 14:52:30 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(2, 192.168.1.83, 35521, None)
19/01/31 14:52:30 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(2, 192.168.1.83, 35521, None)
19/01/31 14:52:30 INFO BlockManager: Initialized BlockManager: BlockManagerId(2, 192.168.1.83, 35521, None)
19/01/31 14:57:46 INFO CoarseGrainedExecutorBackend: Got assigned task 2
19/01/31 14:57:46 INFO CoarseGrainedExecutorBackend: Got assigned task 10
19/01/31 14:57:46 INFO CoarseGrainedExecutorBackend: Got assigned task 17
19/01/31 14:57:46 INFO Executor: Running task 2.0 in stage 0.0 (TID 2)
19/01/31 14:57:46 INFO Executor: Running task 10.0 in stage 0.0 (TID 10)
19/01/31 14:57:46 INFO Executor: Running task 17.0 in stage 0.0 (TID 17)
```

**2.从主节点获取运行代码及其依赖,配置文件等**
* 依次将.zip,.py, .conf等文件从主节点的files/目录下获取,缓存到本地,再拷贝到形如/home/hadoop/spark/work/app-20190131145228-0005/2/./的目录下
```
9/01/31 14:57:46 INFO Executor: Fetching spark://192.168.1.122:42737/files/ZM_Lab_Alpha_System.zip with timestamp 1548917548577
19/01/31 14:57:46 INFO TransportClientFactory: Successfully created connection to /192.168.1.122:42737 after 3 ms (0 ms spent in bootstraps)
19/01/31 14:57:46 INFO Utils: Fetching spark://192.168.1.122:42737/files/ZM_Lab_Alpha_System.zip to /tmp/spark-00ad7881-80c2-451f-9573-1e52478e74f5/executor-c6674c9e-c231-4e70-a739-2bf8cf5da17e/spark-336d0426-858a-49ad-a4db-90c678093461/fetchFileTemp1776261837924149178.tmp
19/01/31 14:57:50 INFO Utils: Copying /tmp/spark-00ad7881-80c2-451f-9573-1e52478e74f5/executor-c6674c9e-c231-4e70-a739-2bf8cf5da17e/spark-336d0426-858a-49ad-a4db-90c678093461/-20204704431548917548577_cache to /home/hadoop/spark/work/app-20190131145228-0005/2/./ZM_Lab_Alpha_System.zip
19/01/31 14:57:50 INFO Executor: Fetching spark://192.168.1.122:42737/files/spark_tuning.py with timestamp 1548917548564
19/01/31 14:57:50 INFO Utils: Fetching spark://192.168.1.122:42737/files/spark_tuning.py to /tmp/spark-00ad7881-80c2-451f-9573-1e52478e74f5/executor-c6674c9e-c231-4e70-a739-2bf8cf5da17e/spark-336d0426-858a-49ad-a4db-90c678093461/fetchFileTemp3946925521355699257.tmp
19/01/31 14:57:50 INFO Utils: Copying /tmp/spark-00ad7881-80c2-451f-9573-1e52478e74f5/executor-c6674c9e-c231-4e70-a739-2bf8cf5da17e/spark-336d0426-858a-49ad-a4db-90c678093461/-9296098361548917548564_cache to /home/hadoop/spark/work/app-20190131145228-0005/2/./spark_tuning.py
19/01/31 14:57:50 INFO Executor: Fetching spark://192.168.1.122:42737/files/mongo_211_ZMingStopLose.conf with timestamp 1548917549017
19/01/31 14:57:50 INFO Utils: Fetching spark://192.168.1.122:42737/files/mongo_211_ZMingStopLose.conf to /tmp/spark-00ad7881-80c2-451f-9573-1e52478e74f5/executor-c6674c9e-c231-4e70-a739-2bf8cf5da17e/spark-336d0426-858a-49ad-a4db-90c678093461/fetchFileTemp6305071164934492107.tmp
19/01/31 14:57:50 INFO Utils: Copying /tmp/spark-00ad7881-80c2-451f-9573-1e52478e74f5/executor-c6674c9e-c231-4e70-a739-2bf8cf5da17e/spark-336d0426-858a-49ad-a4db-90c678093461/3143562401548917549017_cache to /home/hadoop/spark/work/app-20190131145228-0005/2/./mongo_211_ZMingStopLose.conf
```
**3. 从主节点和其他slave获得数据进行分布式计算**
* 主节点将varible1和varible2打散,分发给各个slave
* slave机又从各个slave机中得到varible1和varible2,共花费了118007ms
```
19/01/31 14:57:50 INFO TorrentBroadcast: Started reading broadcast variable 1
19/01/31 14:57:50 INFO TransportClientFactory: Successfully created connection to /192.168.1.122:41227 after 2 ms (0 ms spent in bootstraps)
19/01/31 14:57:50 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 2.7 KB, free 366.3 MB)
19/01/31 14:57:50 INFO TorrentBroadcast: Reading broadcast variable 1 took 331 ms
19/01/31 14:57:50 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 4.2 KB, free 366.3 MB)
19/01/31 14:57:51 INFO TorrentBroadcast: Started reading broadcast variable 0
19/01/31 14:57:53 INFO MemoryStore: Block broadcast_0_piece52 stored as bytes in memory (estimated size 4.0 MB, free 362.3 MB)
19/01/31 14:57:58 INFO MemoryStore: Block broadcast_0_piece3 stored as bytes in memory (estimated size 4.0 MB, free 358.3 MB)
19/01/31 14:58:01 INFO MemoryStore: Block broadcast_0_piece53 stored as bytes in memory (estimated size 4.0 MB, free 354.3 MB)
19/01/31 14:58:03 INFO MemoryStore: Block broadcast_0_piece34 stored as bytes in memory (estimated size 4.0 MB, free 350.3 MB)
19/01/31 14:58:03 INFO TransportClientFactory: Successfully created connection to /192.168.1.122:35713 after 4 ms (0 ms spent in bootstraps)
19/01/31 14:58:29 INFO MemoryStore: Block broadcast_0_piece43 stored as bytes in memory (estimated size 4.0 MB, free 346.3 MB)
19/01/31 14:58:32 INFO MemoryStore: Block broadcast_0_piece31 stored as bytes in memory (estimated size 4.0 MB, free 342.3 MB)
19/01/31 14:58:35 INFO MemoryStore: Block broadcast_0_piece11 stored as bytes in memory (estimated size 4.0 MB, free 338.3 MB)
19/01/31 14:58:37 INFO MemoryStore: Block broadcast_0_piece25 stored as bytes in memory (estimated size 4.0 MB, free 334.3 MB)
19/01/31 14:58:40 INFO MemoryStore: Block broadcast_0_piece51 stored as bytes in memory (estimated size 4.0 MB, free 330.3 MB)
19/01/31 14:58:43 INFO MemoryStore: Block broadcast_0_piece2 stored as bytes in memory (estimated size 4.0 MB, free 326.3 MB)
19/01/31 14:58:46 INFO MemoryStore: Block broadcast_0_piece6 stored as bytes in memory (estimated size 4.0 MB, free 322.3 MB)
19/01/31 14:58:46 INFO TransportClientFactory: Successfully created connection to /192.168.1.115:40271 after 1 ms (0 ms spent in bootstraps)
19/01/31 14:58:46 INFO MemoryStore: Block broadcast_0_piece57 stored as bytes in memory (estimated size 4.0 MB, free 318.3 MB)
19/01/31 14:58:46 INFO TransportClientFactory: Successfully created connection to /192.168.1.85:34706 after 1 ms (0 ms spent in bootstraps)
19/01/31 14:58:47 INFO MemoryStore: Block broadcast_0_piece59 stored as bytes in memory (estimated size 4.0 MB, free 314.3 MB)
19/01/31 14:58:49 INFO MemoryStore: Block broadcast_0_piece36 stored as bytes in memory (estimated size 4.0 MB, free 310.3 MB)
19/01/31 14:58:52 INFO MemoryStore: Block broadcast_0_piece4 stored as bytes in memory (estimated size 4.0 MB, free 306.3 MB)
19/01/31 14:58:53 INFO MemoryStore: Block broadcast_0_piece21 stored as bytes in memory (estimated size 4.0 MB, free 302.3 MB)
19/01/31 14:58:56 INFO MemoryStore: Block broadcast_0_piece38 stored as bytes in memory (estimated size 4.0 MB, free 298.3 MB)
19/01/31 14:58:56 INFO TransportClientFactory: Successfully created connection to /192.168.1.15:33263 after 2 ms (0 ms spent in bootstraps)
19/01/31 14:58:56 INFO MemoryStore: Block broadcast_0_piece23 stored as bytes in memory (estimated size 4.0 MB, free 294.3 MB)
19/01/31 14:58:59 INFO MemoryStore: Block broadcast_0_piece48 stored as bytes in memory (estimated size 4.0 MB, free 290.3 MB)
19/01/31 14:59:02 INFO MemoryStore: Block broadcast_0_piece60 stored as bytes in memory (estimated size 4.0 MB, free 286.3 MB)
19/01/31 14:59:04 INFO MemoryStore: Block broadcast_0_piece13 stored as bytes in memory (estimated size 4.0 MB, free 282.3 MB)
19/01/31 14:59:05 INFO MemoryStore: Block broadcast_0_piece16 stored as bytes in memory (estimated size 4.0 MB, free 278.3 MB)
19/01/31 14:59:06 INFO MemoryStore: Block broadcast_0_piece45 stored as bytes in memory (estimated size 4.0 MB, free 274.3 MB)
19/01/31 14:59:09 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 4.0 MB, free 270.3 MB)
19/01/31 14:59:10 INFO TransportClientFactory: Successfully created connection to /192.168.1.16:40491 after 1 ms (0 ms spent in bootstraps)
19/01/31 14:59:10 INFO MemoryStore: Block broadcast_0_piece26 stored as bytes in memory (estimated size 4.0 MB, free 266.3 MB)
19/01/31 14:59:13 INFO MemoryStore: Block broadcast_0_piece44 stored as bytes in memory (estimated size 4.0 MB, free 262.3 MB)
19/01/31 14:59:14 INFO MemoryStore: Block broadcast_0_piece40 stored as bytes in memory (estimated size 4.0 MB, free 258.3 MB)
19/01/31 14:59:14 INFO MemoryStore: Block broadcast_0_piece32 stored as bytes in memory (estimated size 4.0 MB, free 254.3 MB)
19/01/31 14:59:15 INFO MemoryStore: Block broadcast_0_piece19 stored as bytes in memory (estimated size 4.0 MB, free 250.3 MB)
19/01/31 14:59:16 INFO MemoryStore: Block broadcast_0_piece35 stored as bytes in memory (estimated size 4.0 MB, free 246.3 MB)
19/01/31 14:59:16 INFO MemoryStore: Block broadcast_0_piece17 stored as bytes in memory (estimated size 4.0 MB, free 242.3 MB)
19/01/31 14:59:18 INFO MemoryStore: Block broadcast_0_piece24 stored as bytes in memory (estimated size 4.0 MB, free 238.3 MB)
19/01/31 14:59:20 INFO MemoryStore: Block broadcast_0_piece50 stored as bytes in memory (estimated size 4.0 MB, free 234.3 MB)
19/01/31 14:59:21 INFO MemoryStore: Block broadcast_0_piece55 stored as bytes in memory (estimated size 4.0 MB, free 230.3 MB)
19/01/31 14:59:21 INFO MemoryStore: Block broadcast_0_piece9 stored as bytes in memory (estimated size 4.0 MB, free 226.3 MB)
19/01/31 14:59:22 INFO MemoryStore: Block broadcast_0_piece5 stored as bytes in memory (estimated size 4.0 MB, free 222.3 MB)
19/01/31 14:59:23 INFO MemoryStore: Block broadcast_0_piece33 stored as bytes in memory (estimated size 4.0 MB, free 218.3 MB)
19/01/31 14:59:25 INFO MemoryStore: Block broadcast_0_piece61 stored as bytes in memory (estimated size 4.0 MB, free 214.3 MB)
19/01/31 14:59:26 INFO MemoryStore: Block broadcast_0_piece22 stored as bytes in memory (estimated size 4.0 MB, free 210.3 MB)
19/01/31 14:59:28 INFO MemoryStore: Block broadcast_0_piece27 stored as bytes in memory (estimated size 4.0 MB, free 206.3 MB)
19/01/31 14:59:29 INFO MemoryStore: Block broadcast_0_piece58 stored as bytes in memory (estimated size 4.0 MB, free 202.3 MB)
19/01/31 14:59:30 INFO MemoryStore: Block broadcast_0_piece10 stored as bytes in memory (estimated size 4.0 MB, free 198.3 MB)
19/01/31 14:59:31 INFO MemoryStore: Block broadcast_0_piece49 stored as bytes in memory (estimated size 4.0 MB, free 194.3 MB)
19/01/31 14:59:31 INFO TransportClientFactory: Successfully created connection to /192.168.1.53:42657 after 11 ms (0 ms spent in bootstraps)
19/01/31 14:59:32 INFO MemoryStore: Block broadcast_0_piece30 stored as bytes in memory (estimated size 4.0 MB, free 190.3 MB)
19/01/31 14:59:33 INFO MemoryStore: Block broadcast_0_piece7 stored as bytes in memory (estimated size 4.0 MB, free 186.3 MB)
19/01/31 14:59:33 INFO MemoryStore: Block broadcast_0_piece29 stored as bytes in memory (estimated size 4.0 MB, free 182.3 MB)
19/01/31 14:59:33 INFO TransportClientFactory: Successfully created connection to /192.168.1.143:36029 after 3 ms (0 ms spent in bootstraps)
19/01/31 14:59:33 INFO MemoryStore: Block broadcast_0_piece41 stored as bytes in memory (estimated size 4.0 MB, free 178.3 MB)
19/01/31 14:59:34 INFO MemoryStore: Block broadcast_0_piece42 stored as bytes in memory (estimated size 4.0 MB, free 174.3 MB)
19/01/31 14:59:36 INFO MemoryStore: Block broadcast_0_piece14 stored as bytes in memory (estimated size 4.0 MB, free 170.3 MB)
19/01/31 14:59:39 INFO MemoryStore: Block broadcast_0_piece28 stored as bytes in memory (estimated size 4.0 MB, free 166.3 MB)
19/01/31 14:59:39 INFO MemoryStore: Block broadcast_0_piece39 stored as bytes in memory (estimated size 4.0 MB, free 162.3 MB)
19/01/31 14:59:41 INFO MemoryStore: Block broadcast_0_piece37 stored as bytes in memory (estimated size 4.0 MB, free 158.3 MB)
19/01/31 14:59:42 INFO MemoryStore: Block broadcast_0_piece1 stored as bytes in memory (estimated size 4.0 MB, free 154.3 MB)
19/01/31 14:59:43 INFO MemoryStore: Block broadcast_0_piece15 stored as bytes in memory (estimated size 4.0 MB, free 150.3 MB)
19/01/31 14:59:43 INFO MemoryStore: Block broadcast_0_piece20 stored as bytes in memory (estimated size 4.0 MB, free 146.3 MB)
19/01/31 14:59:44 INFO MemoryStore: Block broadcast_0_piece18 stored as bytes in memory (estimated size 4.0 MB, free 142.3 MB)
19/01/31 14:59:44 INFO MemoryStore: Block broadcast_0_piece8 stored as bytes in memory (estimated size 4.0 MB, free 138.3 MB)
19/01/31 14:59:45 INFO MemoryStore: Block broadcast_0_piece46 stored as bytes in memory (estimated size 4.0 MB, free 134.3 MB)
19/01/31 14:59:47 INFO MemoryStore: Block broadcast_0_piece12 stored as bytes in memory (estimated size 4.0 MB, free 130.3 MB)
19/01/31 14:59:48 INFO MemoryStore: Block broadcast_0_piece47 stored as bytes in memory (estimated size 4.0 MB, free 126.3 MB)
19/01/31 14:59:48 INFO MemoryStore: Block broadcast_0_piece62 stored as bytes in memory (estimated size 1159.9 KB, free 125.2 MB)
19/01/31 14:59:48 INFO MemoryStore: Block broadcast_0_piece54 stored as bytes in memory (estimated size 4.0 MB, free 121.2 MB)
19/01/31 14:59:49 INFO MemoryStore: Block broadcast_0_piece56 stored as bytes in memory (estimated size 4.0 MB, free 117.2 MB)
19/01/31 14:59:49 INFO TorrentBroadcast: Reading broadcast variable 0 took 118007 ms
19/01/31 14:59:50 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 416.0 B, free 117.2 MB)
```
