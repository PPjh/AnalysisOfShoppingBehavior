-- ODS(原始数据层)
-- 创建并使用数据库
create database subject;
use subject;

drop table if exists AnalysisOfShoppingBehavior;
create external table AnalysisOfShoppingBehavior
(
    Line string
)
    partitioned by (year string)
    location '/AnalysisOfShoppingBehavior/table';

load data inpath '/AnalysisOfShoppingBehavior/data/2020.json' into table AnalysisOfShoppingBehavior
    partition (year = '2020');

load data inpath '/AnalysisOfShoppingBehavior/data/2021.json' into table AnalysisOfShoppingBehavior
    partition (year = '2021');

load data inpath '/AnalysisOfShoppingBehavior/data/2022.json' into table AnalysisOfShoppingBehavior
    partition (year = '2022');

load data inpath '/AnalysisOfShoppingBehavior/data/2023.json' into table AnalysisOfShoppingBehavior
    partition (year = '2023');

-- 导入时加入： Stored as textfile
-- 可以防止hive表第一行为空

-- DWD(数据明细层)
-- 处理业务数据

create function get_json_cn
    as 'UDFs.UDFGetJsonObjectCN' using jar 'hdfs://hadoop102//spark/jars/AnalysisOfShoppingBehavior-1.0-SNAPSHOT.jar';

select get_json_cn(line, "$.销售日期")       data,
       get_json_cn(line, "$.单品名称")       name,
       get_json_cn(line, "$.销量(千克)")     volume,
       get_json_cn(line, "$.销售单价(元/千克)") Price,
       get_json_cn(line, "$.销售类型")       saleClass,
       get_json_cn(line, "$.是否打折销售")     DiscountsOrNOt,
       get_json_cn(line, "$.分类名称")       className
from analysisofshoppingbehavior;

create function replace_bracket
    as 'UDFs.SimplifyName' using jar 'hdfs://hadoop102//spark/jars/AnalysisOfShoppingBehavior-1.0-SNAPSHOT.jar';

select get_json_cn(line, "$.销售日期")                  data,
       replace_bracket(get_json_cn(line, "$.单品名称")) name,
       get_json_cn(line, "$.销量(千克)")                volume,
       get_json_cn(line, "$.销售单价(元/千克)")            Price,
       get_json_cn(line, "$.销售类型")                  saleClass,
       get_json_cn(line, "$.是否打折销售")                DiscountsOrNOt,
       get_json_cn(line, "$.分类名称")                  className
from analysisofshoppingbehavior;

create external table dwd_AOfSB
(
    dt             date,
    name           string,
    volume         double,
    price          double,
    saleClass      string,
    DiscountsOrNOt string,
    className      string
)
    comment '处理json数据列表'
    partitioned by (year string)
    stored as orc
    location '/AnalysisOfShoppingBehavior/splitJsonTable';

-- 开启允许所有分区都是动态的，否则必须要有静态分区才能使用。
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- partition (year)即按year动态分区，不用静态设置year=2020-2023

insert overwrite table dwd_AOfSB partition (year)
select get_json_cn(line, "$.销售日期")                  data,
       replace_bracket(get_json_cn(line, "$.单品名称")) name,
       get_json_cn(line, "$.销量(千克)")                volume,
       get_json_cn(line, "$.销售单价(元/千克)")            Price,
       get_json_cn(line, "$.销售类型")                  saleClass,
       get_json_cn(line, "$.是否打折销售")                DiscountsOrNOt,
       get_json_cn(line, "$.分类名称")                  className,
       year
from analysisofshoppingbehavior;


-- DWS 数据汇总层
-- 按特定规则汇总数据

-- 由于项目数据较为简单，可以直接用DWD层数据

-- DIM 维度层
create external table dim_date
(
    `Date` date,
    year int,
    month int,
    day int,
    quarter int,
    `dayOfWeek` int
)
row format delimited fields terminated by ','
    lines terminated by '\n'
LOCATION '/AnalysisOfShoppingBehavior/dim/dim_date';
-- csv文件格式化为hive表
-- 可以用
-- row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

load data inpath '/AnalysisOfShoppingBehavior/dim/time_dimension_table.csv'
    into table dim_date;

-- ADS 应用层
-- 需求:四年各蔬菜种类各季度销量总和

drop table if exists quarterTotalVolume_byClassName;
create external table quarterTotalVolume_byClassName
(
    quarter string comment "季度",
    className string comment "蔬菜类名",
    volume double comment "总销量"
)
ROW FORMAT DELIMITED fields terminated by '\t'
LOCATION "/AnalysisOfShoppingBehavior/ads/quarterTotalVolume_byClassName";


insert overwrite table quarterTotalVolume_byClassName
SELECT
  dim.quarter,
  dwd.classname ,
  SUM(dwd.volume) AS total_volume
FROM
  dwd_AOfSB AS dwd
INNER JOIN
  dim_date AS dim
ON
  dwd.dt == dim.`Date`
GROUP BY
  dim.quarter,
  dwd.classname;

