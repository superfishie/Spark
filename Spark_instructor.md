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
![avatar](http://pjpf9017m.bkt.clouddn.com/first_test.png)
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
在生成了任务列表之后,将任务列表中的每一个元素作为参数,传入任务处理类中的任务处理方法进行集中处理. 任务处理函数可以是静态函数,也可以是普通方法或静态方法. **唯一的要求是:任务处理函数的参数只有一个且参数类型只能是dict**

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
![avatar](http://pjpf9017m.bkt.clouddn.com/open_t.png)      
2.Ctrl+A 将项目目录下的文件全部打包成.zip文件,注意不是将项目文件夹打包而是将项目文件夹下的文件夹打包
![avatar](http://pjpf9017m.bkt.clouddn.com/zip.png)      
3.在终端中输入: ```/home/hadoop/spark/bin/spark-submit --master spark://liuqiang-pc:7077 --py-files ZM_Lab_Alpha_System.zip development/alpha_production/dp/run_spark_dp6.py```
![avatar](http://pjpf9017m.bkt.clouddn.com/run.png)  
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
### 3.2 关于slave路径问题
对 slave 机器来说，都是在形如：/home/hadoop/spark/work/app-20181211182511-0001/3 的临时路径运行，此位置即为工作目录。(关于这一点，大家可以从自己终端的打印信息中看到他的创建和销毁过程)
同样的，打包后的zip文件也会被放置在这个路径下。
```
print(os.path.abspath(os.path.realpath('.')))
print(os.getcwd())
print(os.path.abspath(os.path.realpath(__file__)))

"""
/home/hadoop/spark/work/app-20181211182511-0001/3
/home/hadoop/spark/work/app-20181211182511-0001/3
/home/hadoop/spark/work/app-20181211182511-0001/3/ZM_Lab_Alpha_System.zip/development/alpha_production/ap_util/db_operation.py
"""
```
### 3.3 关于依赖的问题
1. 使用者需要理解的是，使用spark运行Python代码时，不论这行代码是运行在master节点或是slave节点，他寻找一切依赖的自定义模块时，都是从zip中寻找。
2. 配置文件不能直接被找到,需要在命令行中用“--py-files zip的路径” 与 “--files 配置文件的路径” ,或在代码中使用 sc.addFile() 和 sc.addPyFile() 替代。
 