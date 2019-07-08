# SqlLog
使用sql读取,分析日志文件

平时查看生产的日志文件是直接通过grep,tail等命令搜索的,命令多不好记,就想使用sql来查询,可读性更强更方便一些.

支持ssh host "cmd" 模式,这样不用登陆就能查看日志
支持linux和mac,不支持windows是因为没windows电脑,我不知道能不能跑
支持linux管道命令

# 使用方法
代码用java写的,需要先安装java环境

下载代码编译:
git clone https://github.com/meeepo/SqlLog.git
cd SqlLog
javac SqlLog.java 

使用测试数据查看效果:
java SqlLog "select * from testData in path local://local$(pwd) where match aaa limit 1,3"

使用管道:
echo "aa1\naa2\naa3" | java SqlLog "select 'aa(\d)' 'find para #1=#{1}' from pipe where #1=2"

# 完整语法示例

select 'regx with group like (\d) ' 'format string like ${1} or group function count(${1})'
from pipe|filename1,filename2
in path 'local://local/path/path1','ssh://host/path/path2'
where match 'regx' not match regx
and #1=4 and #1>=3 and #1 in (1,2,3) and #1 not in (1,2,3)
order by #1,#2
group by #1,#2
limit 10,20

# 配置参数

-follow : 跟踪文件追加的输出,使用tail -f 实现
-tail : 正常是从前往后读文件(使用cat命令),-tail则是从末尾向前读(使用tac命令)
-ignoreCase : where match regx 忽略大小写,使用grep -i -e实现
-maxLine=10000 : order by group by 功能需要在内存中缓存数据,maxLine为最大缓存数据行数,防止内存溢出
-preLimit=10,5 : 过滤掉前5行输入,然后只处理随后5行输入,这个过滤在一切过滤条件之前

# 其他说明
可以使用select * ,select count(*)
管道操作需要设置from pipe ,且不需要in path 语句.文件操作可以设置多个文件,用逗号分隔.
where,order by,group by limit 都为可选字段
where 条件支持 >,<,>=,<=,!=,=,in,not in,match,not match

select 语句有2个参数,前一个为一个正则表达式,后一个为用来格式化输出的字符串
例如:
输入行[aaa1,bbb2,ccc3]
select 'aaa(\d),bbb(\d),ccc' '#{0}|#{1}|${2}'
则${0}=aaa(\d),bbb(\d),ccc  ${1}=1 ${2}=2 ${3}=3
输出:  aaa(\d),bbb(\d),ccc|1|2|3
其中的分组序号为变量,如#0代表匹配的全部字符,#1代表第一个匹配组内容
需要注意的是,select 重的变量必须为${1}格式,where,order,group 可以使用#{1}或者#1格式

聚合函数支持count(),sum(),max(),min(),avg(),使用时,格式化字符串写为‘sum(${1})’




