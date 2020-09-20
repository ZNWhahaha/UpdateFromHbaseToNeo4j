package com.znwhahaha;

import org.apache.log4j.Logger;
import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Str;
import org.neo4j.driver.internal.InternalDriver;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import scala.Int;
import org.neo4j.driver.v1.Session;

import java.io.File;
import java.util.ArrayList;

public class ToNeo4j {
    private static Logger logger = Logger.getLogger(ToNeo4j.class);

    //数据库实例
    GraphDatabaseService graphDB ;
    GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
//    Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "neo4j"));
//    private Session session = driver.session();

    /**
     * @ClassName : ToNeo4j
     * @Description :创建关系类型
     * @param

     * @Return :
     * @Author : ZNWhahaha
     * @Date : 2020/6/10
     */
    public enum RelTypes implements RelationshipType {
        Contain,Belongto,Link
    }

    /**
     * @ClassName : ToNeo4j
     * @Description : 创建节点类型
     * @param

     * @Return :
     * @Author : ZNWhahaha
     * @Date : 2020/6/10
     */
    public enum NodeType implements Label {
        Author,Organization,Paper,Location,Instrument,Achievement
    }

    /**
     * @ClassName : ToNeo4j
     * @Description : 初始化Neo4j数据库
     * @param

     * @Return : void
     * @Author : ZNWhahaha
     * @Date : 2020/6/18
     */
    public void init(String path) {

        graphDB = dbFactory.newEmbeddedDatabase(new File(path));
    }

    /**
     * @ClassName : ToNeo4j
     * @Description : 通过属性进行查询，验证是否存在
     * @param : name

     * @Return : java.lang.Boolean
     * @Author : ZNWhahaha
     * @Date : 2020/6/18
     */
    public ArrayList<Node> search(NodeType nodetype, String type, String name){
        logger.info("开始查找Node : " +name);
        logger.info("type : " + type);
        ArrayList<Node> nodes = new ArrayList<>();
        Transaction tx = graphDB.beginTx();
        ResourceIterator<Node> node = graphDB.findNodes(nodetype, type, name);
        while (node.hasNext()){
            nodes.add(node.next());
        }
        tx.success();
        tx.close();
        logger.info("查找Node结束");
        return nodes;
    }

    /**
     * @ClassName : ToNeo4j
     * @Description : 对于相应节点创建对应关系
     * @param node_1
     * @param node_2
     * @param
     * @Return : void
     * @Author : ZNWhahaha
     * @Date : 2020/6/18
     */
    public void createRel(Node node_1,Node node_2){
        logger.info("开始创建关系 ");
        Transaction tx = graphDB.beginTx();
        Relationship r1 =  node_1.createRelationshipTo(node_2,RelTypes.Link);
        r1.setProperty("Alone","YES");
        tx.success();
        tx.close();
        logger.info("创建关系结束");
    }

    /**
     * @ClassName : ToNeo4j
     * @Description : 创建成果节点
     * @param name
     * @param mainpeople
     * @param mainunit
     * @param reward
     * @param origin
     * @Return : void
     * @Author : ZNWhahaha
     * @Date : 2020/6/19
     */
    public Node createAchieveNode(String name,int id,String mainpeople,String mainunit,String reward,String origin) {
        logger.info("开始创建AchieveNode : " + name);
        Transaction tx = graphDB.beginTx();
        Node node = graphDB.createNode(NodeType.Achievement);
        node.setProperty("name", name);
        node.setProperty("achid", id);
        node.setProperty("mainpeople", mainpeople);
        node.setProperty("mainunit", mainunit);
        node.setProperty("reward", reward);
        node.setProperty("origin", origin);
        logger.info("创建AchieveNode结束");
        tx.success();
        tx.close();
        
        return node;

    }

    /**
     * @ClassName : ToNeo4j
     * @Description : 创建Instrument节点
     * @param name
     * @param org
     * @param mainpeople
     * @param manufacturer
     * @param price
     * @Return : org.neo4j.graphdb.Node
     * @Author : ZNWhahaha
     * @Date : 2020/6/19
     */
    public Node createInstrumentNode(String name,int id,String org,String mainpeople,String manufacturer,String price){
        logger.info("开始创建AchieveNode : " + name);
        Transaction tx = graphDB.beginTx();
        Node node = graphDB.createNode(NodeType.Instrument);
        node.setProperty("name", name);
        node.setProperty("instid", id);
        node.setProperty("organization", org);
        node.setProperty("mainpeople", mainpeople);
        node.setProperty("manufacturer", manufacturer);
        node.setProperty("price", price);
        logger.info("创建AchieveNode结束");
        tx.success();
        tx.close();
        return node;
    }

    /**
     * @ClassName : ToNeo4j
     * @Description : 创建Person节点
     * @param nameEn
     * @param name
     * @param personID
     * @param personID2
     * @Return : Node
     * @Author : ZNWhahaha
     * @Date : 2020/6/18
     */
    public Node createPersonNode(String name, String nameEn, int id, String personID, String personID2, String persontype, String org){
        logger.info("开始创建PersonNode : "+ name);
        Transaction tx = graphDB.beginTx();

        Node node = graphDB.createNode(NodeType.Author);
        node.setProperty("name", name);
        node.setProperty("nameEn", nameEn);
        node.setProperty("authorID",id);
        node.setProperty("personType",persontype);
        node.setProperty("organization",org);
        node.setProperty("personID", personID);
        if (!personID.equals(personID2)){
            node.setProperty("personID2", personID2);
        }
        logger.info("PersonNode创建结束");
        tx.success();
        tx.close();
        return node;
    }

    /**
     * @ClassName : ToNeo4j
     * @Description : 创建Paper节点
     * @param papername
     * @param paperID
     * @Return : Node
     * @Author : ZNWhahaha
     * @Date : 2020/6/18
     */
    public Node createPaperNode(String papername,String paperID){
        logger.info("开始创建PaperNode : " + papername);
        Transaction tx = graphDB.beginTx();
        Node node = graphDB.createNode(NodeType.Paper);
        node.setProperty("name", papername);
        node.setProperty("paperID", paperID);
        tx.success();
        tx.close();
        logger.info("创建PaperNode结束");
        return node;
    }

    /**
     * @ClassName : ToNeo4j
     * @Description : 创建org节点
     * @param orgname
     * @param orgmessage
     * @Return : Node
     * @Author : ZNWhahaha
     * @Date : 2020/6/18
     */
    public Node createOrgNode(String orgname,int id,String orgmessage){
        logger.info("开始创建OrgNode : " + orgname);
        Transaction tx = graphDB.beginTx();
        Node node = graphDB.createNode(NodeType.Organization);
        node.setProperty("name", orgname);
        node.setProperty("orgid", id);
        if(orgmessage.contains(",")){
            node.setProperty("orgmessage", orgmessage.split(",")[0]);
            node.setProperty("orgpostcode", orgmessage.split(",")[1]);
        }
        else {
            node.setProperty("orgmessage", orgmessage);
        }
        logger.info("创建OrgNode结束");
        tx.success();
        tx.close();
        return node;

    }

    /**
     * @ClassName : ToNeo4j
     * @Description : 功能说明: 通过节点类型，查找所有节点，获取相应id
     * @param nodetype

     * @Return : int
     * @Author : ZNWhahaha
     * @Date : 2020/9/15
    */
    public int GetMaxID(NodeType nodetype,String idname){
        logger.info("开始统计最大id : " + nodetype);
        int maxid = 0;
        int nodeid = 0;
        Transaction tx = graphDB.beginTx();
        ResourceIterator<Node> nodes = graphDB.findNodes(nodetype);
        while (nodes.hasNext()){
            Node node = nodes.next();
            nodeid =  (int)node.getProperty(idname);
            if (maxid < nodeid){
                maxid = nodeid;
            }
        }
        tx.success();
        tx.close();
        logger.info("统计最大id结束");
        return maxid;
    }

    /**
     * @ClassName : ToNeo4j
     * @Description : 功能说明:通过属性名获取属性值
     * @param node
     * @param name
     * @Return : java.lang.Object
     * @Author : ZNWhahaha
     * @Date : 2020/9/16
    */
    public Object GetPropertyByName(Node node,String name){
        Transaction tx = graphDB.beginTx();
        Object oj = node.getProperty(name);
//        System.out.println("333333:  "+ node.getProperties(name).get(0));
//        System.out.println("444444:  "+ node.getProperty(name));
        tx.success();
        tx.close();
        return oj;
    }

    /**
     * @ClassName : ToNeo4j
     * @Description : 功能说明: 检查关系是否已经建立，当没有建立时返回true，建立返回false
     * @param node1
     * @param node2
     * @param type
     * @Return : java.lang.Boolean
     * @Author : ZNWhahaha
     * @Date : 2020/9/16
    */
    public Boolean CheckREL(Node node1,Node node2,RelTypes type){
        Transaction tx = graphDB.beginTx();
        Iterable<Relationship> a = node1.getRelationships(type);
        Boolean flag = true;
        for(Relationship n:a){
            if(n.getEndNode().getId() == node2.getId()){
                 flag = false;
            }
        }
        tx.success();
        tx.close();
        return flag;
    }
}
