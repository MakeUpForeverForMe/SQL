// 启动 sql形式
export HADOOP_CONF_DIR=/usr/hdp/3.1.0.0-78/hadoop/etc/hadoop
export CLASSPATH=$HADOOP_CONF_DIR
/home/hdfs/janusgraph-0.4.0/bin/gremlin.sh -i /home/hdfs/janusgraph-0.4.0/northwind.groovy


// 创建 sql形式
graph = NorthwindFactory.createGraph()
g = graph.traversal()



// 远程连接
:remote connect tinkerpop.server conf/remote.yaml
:remote console


// 应用于 Gephi
:plugin use tinkerpop.gephi
:remote connect tinkerpop.gephi
:remote config host 10.10.18.48
:remote config workspace janusgraph
graph = TinkerFactory.createModern()
:> graph




// 显示变量
:show variables

// 众神之图
// 入口
:plugin use tinkerpop.hadoop
:plugin use tinkerpop.spark

graph = GraphFactory.open('conf/hadoop-graph/spark-hbase-es.proper')
g = graph.traversal().withComputer(SparkGraphComputer)

g.V().count()

g.E().count()



:remote connect tinkerpop.hadoop graph g
:> g.V().group().by{it.value('name')[1]}.by('name')


// 加载众神之图
graph = JanusGraphFactory.open('conf/http-janusgraph-hbase-es.properties')

GraphOfTheGodsFactory.load(graph)

g = graph.traversal()

// 点查询
g.V()
v[4128]
v[4152]
v[8248]
v[12344]
v[4184]
v[4248]
v[8344]
v[4288]
v[8384]
v[12480]
v[16576]
v[20672]

// 显示50个节点的详细信息
g.V().valueMap(true).limit(50)
// 显示前50个节点name属性
g.V().values('name').limit(50)
// 会将属性显示出来
g.V().properties('name').limit(50)
// 根据系统id：1234 查询name属性，可以直接 .属性() 查询
g.V('1234').values('name') // = g.V().name()

// 边查询
g.E()
e[5639][12344-lives->8344]
e[2571][4184-lives->8248]
e[3083][4184-brother->4288]
e[3595][4184-brother->12480]
e[10264][4288-lives->4128]
e[9752][4288-father->4152]
e[10776][4288-brother->4184]
e[11288][4288-brother->12480]
e[12312][8384-mother->4248]
e[11800][8384-father->4288]
e[13848][8384-battled->12344]
e[12824][8384-battled->16576]
e[13336][8384-battled->20672]
e[15384][12480-lives->8344]
e[14872][12480-brother->4184]
e[14360][12480-brother->4288]
e[15896][12480-pet->12344]

// 显示50个节点的详细信息
g.E().valueMap(true).limit(50)
// properties与values基本相同，但是properties更详细
g.E().properties().limit(50)
g.E().values().limit(50)
// 查询label
g.E().label().limit(50)

// 获取 顶点id 为 12344 的边为 lives 的边信息
g.V(12344).outE('lives').valueMap(true)
[id:5639,label:lives]
g.V(12344).outE('lives')
e[5639][12344-lives->8344]

// 获取 边为 lives 的边信息
g.V().outE('lives')
e[5639][12344-lives->8344]
e[2571][4184-lives->8248]
e[10264][4288-lives->4128]
e[15384][12480-lives->8344]

g.V(12344).outE('lives').otherV()
g.V(12344).out('lives').values('name')

g.V('20672').outE().otherV().limit(50).valueMap(true)


g.V().valueMap(true).limit(50)

g.V('20672').valueMap(true)

g.V('4248').outE().otherV().valueMap(true)







// 自定义三国杀
// 创建入口
graph = HadoopGraph.open('conf/hadoop-graph/spark-hbase-es.proper')
graph = JanusGraphFactory.open('conf/http-janusgraph-hbase-es.properties')


// 创建顶点标签
mgmt = graph.openManagement()
mgmt.makeVertexLabel('person').make()
mgmt.makeVertexLabel('country').make()
mgmt.makeVertexLabel('weapon').make()
mgmt.getVertexLabels()
mgmt.commit()

// 创建边标签
mgmt = graph.openManagement()
brother = mgmt.makeEdgeLabel("brother").make()
mgmt.makeEdgeLabel("battled").make()
mgmt.makeEdgeLabel("belongs").make()
mgmt.makeEdgeLabel("use").make()
mgmt.getRelationTypes(EdgeLabel.class)
mgmt.commit()

// 创建属性
mgmt = graph.openManagement()
name = mgmt.makePropertyKey('name').dataType(String.class).cardinality(Cardinality.SET).make()
mgmt.buildIndex('nameUnique', Vertex.class).addKey(name).unique().buildCompositeIndex()
age = mgmt.makePropertyKey("age").dataType(Integer.class).make()
mgmt.buildIndex('age2', Vertex.class).addKey(age).buildMixedIndex("janusgraph")
mgmt.getGraphIndexes(Vertex.class)
mgmt.commit()

// 添加顶点
g = graph.traversal()

liubei = g.addV("person").property('name','刘备').property('age',45)
guanyu = g.addV("person").property('name','关羽').property('age',42)
zhangfei = g.addV("person").property('name','张飞').property('age',40)
lvbu = g.addV("person").property('name','吕布').property('age',38)
g.addV("country").property('name','蜀国')
g.addV("weapon").property('name','方天画戟')
g.addV("weapon").property('name','双股剑')
g.addV("weapon").property('name','青龙偃月刀')
g.addV("weapon").property('name','丈八蛇矛')

for (tx in graph.getOpenTransactions()) tx.commit()

// 添加关系
g.addE('brother').from(g.V(4112)).to(g.V(8208))
g.addE('brother').from(g.V(4112)).to(g.V(4280))
g.addE('brother').from(g.V(4280)).to(g.V(4112))
g.addE('brother').from(g.V(8208)).to(g.V(4112))
g.addE('brother').from(g.V(8208)).to(g.V(4280))
g.addE('brother').from(g.V(4280)).to(g.V(8208))

g.addE('use').from(g.V(4112)).to(g.V(4312))
g.addE('use').from(g.V(4280)).to(g.V(4320))
g.addE('use').from(g.V(8208)).to(g.V(4152))
g.addE('use').from(g.V(4264)).to(g.V(4160))

g.addE('belongs').from(g.V(4112)).to(g.V(8360))
g.addE('belongs').from(g.V(4280)).to(g.V(8360))
g.addE('belongs').from(g.V(8208)).to(g.V(8360))

g.addE('battled').from(g.V(4264)).to(g.V(4112))
g.addE('battled').from(g.V(4264)).to(g.V(4280))
g.addE('battled').from(g.V(4264)).to(g.V(8208))
g.addE('battled').from(g.V(4112)).to(g.V(4264))
g.addE('battled').from(g.V(4280)).to(g.V(4264))
g.addE('battled').from(g.V(8208)).to(g.V(4264))

for (tx in graph.getOpenTransactions()) tx.commit()


// gremlin查看顶点数和关系数 9 19
g.V().count()
g.E().count()


查询刘备的兄弟
g.V().has('name','刘备').next() // 返回 ==>v[4112]
g.V(4112).out('brother').values()

查询蜀国的所有人物
g.V().has('name','蜀国').next() // 返回 ==>v[8360]
g.V(8360).in('belongs').valueMap()
