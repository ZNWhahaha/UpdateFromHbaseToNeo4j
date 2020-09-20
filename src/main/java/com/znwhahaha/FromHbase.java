package com.znwhahaha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Str;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FromHbase {
    //日志记录
    private static Logger logger = Logger.getLogger(FromHbase.class);

    //与HBase数据库的连接对象
    public static Connection connection;

    //数据库元数操作对象
    public static Admin admin;


    //进行neo4j数据库的创建
    ToNeo4j neo = new ToNeo4j();
    int orgid = 0;
    
    /**
     * @ClassName : FromHbase
     * @Description : hbase 链接设置
     * @param

     * @Return : void
     * @Author : ZNWhahaha
     * @Date : 2020/6/1
     */
    public void setup() throws IOException {
        logger.info("开始Hbase链接配置");
        //取得一个数据库配置参数对象
        Configuration conf =HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
        logger.info("Hbase链接配置完成");
        //创建链接neo4j数据库
        neo.init("/Users/znw_mac/App/neo4j-community-3.5.18/data/databases/graph.db");
        orgid =neo.GetMaxID(ToNeo4j.NodeType.Organization,"orgid");
    }

    //关闭Hbase数据库
    public void close() throws IOException {
        admin.close();
        connection.close();
        logger.info("Hbase数据库已关闭");
    }

    /**
     * @ClassName : FromHbase
     * @Description : 通过表名查询表中的所有数据，转存至Neo4j中
     * @param tableNameString  表名

     * @Return : void
     * @Author : ZNWhahaha
     * @Date : 2020/6/1
     */
    public void queryNewTableFromPersonProfile(String tableNameString) throws IOException {
        logger.info("********开始插入Personfile中新数据********");
        String name = "";
        String nameEN = "";
        String orgName = "";
        String orgMessage = "";
        String personID="";
        String personID2 = "";
        String personType = "";
        List<String> paperID = new ArrayList<String>();
        List<String> paperTitle = new ArrayList<String>();
        int id = neo.GetMaxID(ToNeo4j.NodeType.Author,"authorID");
//        System.out.println("PeopleID:  " + id);
//        System.out.println("OrgID:  " + orgid);
        //获取数据表对象
        Table table = connection.getTable(TableName.valueOf(tableNameString));

        //获取表中的数据
        ResultScanner scanner = table.getScanner(new Scan());

        //循环输出表中的数据
        for (Result result : scanner) {

            String row = Bytes.toString(result.getRow());
            if (row.contains("->")) {
                name = row.split("->")[0];
                orgName = row.split("->")[1];
            }
            else {
                name = row;
                orgName = "";
            }

            logger.info("row key is:"+row);
            logger.info("name : "+name);
            logger.info("orgName : "+ orgName);

            paperID.clear();
            paperTitle.clear();
            List<Cell> listCells = result.listCells();
            for (Cell cell : listCells) {


                //取对应值
                if ("nameEN".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    nameEN = Bytes.toString(CellUtil.cloneValue(cell));
                }
                else if ("orgMessage".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    orgMessage = Bytes.toString(CellUtil.cloneValue(cell));
                }
                else if ("PersonID".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    personID = Bytes.toString(CellUtil.cloneValue(cell));
                }
                else if ("PersonID2".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    personID2 = Bytes.toString(CellUtil.cloneValue(cell));
                }
                else if ("paper".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
                    paperID.add(Bytes.toString(CellUtil.cloneQualifier(cell)));
                    paperTitle.add(Bytes.toString(CellUtil.cloneValue(cell)));

                }
                else {
                    personType = Bytes.toString(CellUtil.cloneValue(cell));
                }
            }
            logger.info("Message are :" +nameEN +"  "+orgMessage+"  "+ personID+" "+personID2+"  "+paperID+"  "+paperTitle+"  "+personType);



            Node personnode = null;
            ArrayList<Node> person = neo.search(ToNeo4j.NodeType.Author,"name",name);

            //查看作者的组织是否符合
            if (!person.isEmpty()){
                Boolean flag = true;
                for(Node nn : person){
                    personnode = nn;

                    String orgcheck = (String) neo.GetPropertyByName(personnode,"organization");
//                    System.out.println("2222222: "+ orgcheck);
                    if (orgcheck == null && orgName == null){
                        flag = false;
                        break;
                    }
                    else if (orgcheck.equals(orgName)){
                        flag = false;
                        break;
                    }
                }
                if (flag == true){
                    id += 1;
                    personnode = neo.createPersonNode(name,nameEN,id,personID,personID2,personType,orgName);

                    //创建Paper节点
                    List<Node> papernode = new ArrayList<Node>();
                    for (int i = 0; i < paperID.size(); i++) {
                        ArrayList<Node> paper = neo.search(ToNeo4j.NodeType.Paper,"name",paperTitle.get(i));
                        if (paper.isEmpty()){
                            papernode.add(neo.createPaperNode(paperTitle.get(i),paperID.get(i)));
                        }
                        else {
                            for(Node papernn:paper){
                                if (neo.CheckREL(personnode,papernn,ToNeo4j.RelTypes.Link)){
                                    papernode.add(papernn);
                                }
                            }
                        }
                    }
                    //添加Paper与Person依赖
                    for (Node ne : papernode){
                        neo.createRel(personnode,ne);
                    }

                    //创建Org节点
                    if (!orgName.equals("") && orgName != null) {
                        ArrayList<Node> org = neo.search(ToNeo4j.NodeType.Organization,"name",orgName);
                        Node orgNode;
                        if (org.isEmpty()) {
                            orgid += 1;
                            orgNode = neo.createOrgNode(orgName, orgid,orgMessage);
                            neo.createRel(orgNode,personnode);
                        }
                        else {
                            for(Node orgnn:org){
                                orgNode = orgnn;
                                neo.createRel(orgNode,personnode);
                            }
                        }
                    }
                }
                else {
                    //创建Paper节点
                    List<Node> papernode = new ArrayList<Node>();
                    for (int i = 0; i < paperID.size(); i++) {
                        ArrayList<Node> paper = neo.search(ToNeo4j.NodeType.Paper,"name",paperTitle.get(i));
                        if (paper.isEmpty()){
                            papernode.add(neo.createPaperNode(paperTitle.get(i),paperID.get(i)));
                        }
                        else {
                            for(Node papernn:paper){
                                if (personnode != null &&neo.CheckREL(personnode,papernn,ToNeo4j.RelTypes.Link)){
                                    papernode.add(papernn);
                                }
                            }
                        }
                    }
                    //添加Paper与Person依赖
                    for (Node ne : papernode){
                        neo.createRel(personnode,ne);
                    }
                }
            }
            else {
                id += 1;
                personnode = neo.createPersonNode(name,nameEN,id,personID,personID2,personType,orgName);

                //创建Paper节点
                List<Node> papernode = new ArrayList<Node>();
                for (int i = 0; i < paperID.size(); i++) {
                    ArrayList<Node> paper = neo.search(ToNeo4j.NodeType.Paper,"name",paperTitle.get(i));
                    if (paper.isEmpty()){
                        papernode.add(neo.createPaperNode(paperTitle.get(i),paperID.get(i)));
                    }
                    else {
                        for(Node papernn:paper){
                            if (neo.CheckREL(personnode,papernn,ToNeo4j.RelTypes.Link)){
                                papernode.add(papernn);
                            }
                        }
                    }
                }
                //添加Paper与Person依赖
                for (Node ne : papernode){
                    neo.createRel(personnode,ne);
                }

                //创建Org节点
                if (!orgName.equals("") && orgName != null) {
                    ArrayList<Node> org = neo.search(ToNeo4j.NodeType.Organization,"name",orgName);
                    Node orgNode;
                    if (org.isEmpty()) {
                        System.out.println("22222");
                        orgid += 1;
                        orgNode = neo.createOrgNode(orgName, orgid,orgMessage);
                        neo.createRel(orgNode,personnode);
                    }
                    else {
                        for(Node orgnn:org){
                            orgNode = orgnn;
                            neo.createRel(orgNode,personnode);
                        }
                    }
                }


            }

        }
        logger.info("********转存数据结束********");
    }

    /**
     * @ClassName : FromHbase
     * @Description : 通过表名查询表中的所有数据，转存至Neo4j数据库中
     * @param tableNameString

     * @Return : void
     * @Author : ZNWhahaha
     * @Date : 2020/6/19
     */
    public void queryNewTableFromInstrument(String tableNameString) throws IOException {
        logger.info("********开始插入Instrumen新表数据********");
        String name = "";
        String orgname = "";
        String manufacturer = "";
        String mainpeople = "";
        String price = "";
        int id = neo.GetMaxID(ToNeo4j.NodeType.Instrument,"instid");
        //获取数据表对象
        Table table = connection.getTable(TableName.valueOf(tableNameString));

        //获取表中的数据
        ResultScanner scanner = table.getScanner(new Scan());

        //循环输出表中的数据
        for (Result result : scanner) {

            List<Cell> listCells = result.listCells();
            for (Cell cell : listCells) {
                if ("m_instru_ch_name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    name = Bytes.toString(CellUtil.cloneValue(cell));
                }
                else if ("m_instru_organization".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    orgname = Bytes.toString(CellUtil.cloneValue(cell));
                }
                else if ("m_instru_contact_person".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    mainpeople = Bytes.toString(CellUtil.cloneValue(cell));
                }
                else if ("m_instru_manufacturer".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    manufacturer = Bytes.toString(CellUtil.cloneValue(cell));
                }
                else if ("m_instru_price_rmb".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    price = Bytes.toString(CellUtil.cloneValue(cell));
                }
            }
            //判断节点是否存在
            ArrayList<Node> instnodes = neo.search(ToNeo4j.NodeType.Instrument,"name",name);
            if(instnodes.isEmpty()) {
                id += 1;
                Node InstNode = neo.createInstrumentNode(name, id, orgname, mainpeople, manufacturer, price);
                //添加Org与Inst依赖
                ArrayList<Node> org = neo.search(ToNeo4j.NodeType.Organization, "name", orgname);
                if (org.isEmpty()) {
                    orgid += 1;
                    Node orgnode = neo.createOrgNode(orgname, orgid, "");
                    neo.createRel(orgnode, InstNode);
                } else {
                    for (Node orgnn : org) {
                        neo.createRel(orgnn, InstNode);
                    }
                }
            }
            else{
                boolean flag = true;
                for (Node insnn:instnodes){
                    String orgcheck = (String) neo.GetPropertyByName(insnn,"organization");
                    if (orgcheck == null && orgname == null){
                        flag = false;
                        break;
                    }
                    else if (orgcheck.equals(orgname)){
                        flag = false;
                        break;
                    }
                }
                if (flag == true){
                    id += 1;
                    Node InstNode = neo.createInstrumentNode(name, id, orgname, mainpeople, manufacturer, price);
                    //添加Org与Inst依赖
                    ArrayList<Node> org = neo.search(ToNeo4j.NodeType.Organization, "name", orgname);
                    if (org.isEmpty()) {
                        orgid += 1;
                        Node orgnode = neo.createOrgNode(orgname, orgid, "");
                        neo.createRel(orgnode, InstNode);
                    } else {
                        for (Node orgnn : org) {
                            neo.createRel(orgnn, InstNode);
                        }
                    }
                }
            }


        }
        logger.info("********Instrumen表数据转移更新完成********");
    }

    /**
     * @ClassName : FromHbase
     * @Description : 通过表名查询表中的所有数据，转存至Neo4j中
     * @param tableNameString

     * @Return : void
     * @Author : ZNWhahaha
     * @Date : 2020/6/19
     */
    public void queryNewTbaleFromAchievements(String tableNameString) throws IOException {
        logger.info("********开始查询Achievements表数据********");
        String projectname = "";
        String origin = "";
        String mainunit = "";
        String reward = "";
        String mainpeople = "";
        int id = neo.GetMaxID(ToNeo4j.NodeType.Achievement,"achid");
        //获取数据表对象
        Table table = connection.getTable(TableName.valueOf(tableNameString));

        //获取表中的数据
        ResultScanner scanner = table.getScanner(new Scan());

        //循环输出表中的数据
        for (Result result : scanner) {

            List<Cell> listCells = result.listCells();
            for (Cell cell : listCells) {
                if ("project_name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    projectname = Bytes.toString(CellUtil.cloneValue(cell));
                }
                else if ("main_people".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    mainpeople = Bytes.toString(CellUtil.cloneValue(cell));
                }
                else if ("reward_type".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    reward = Bytes.toString(CellUtil.cloneValue(cell));
                }
                else if ("main_unit".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    mainunit = Bytes.toString(CellUtil.cloneValue(cell));
                }
            }
            //判断节点是否存在
            ArrayList<Node> projectnodes = neo.search(ToNeo4j.NodeType.Achievement,"name",projectname);
            if (projectnodes.isEmpty()){
                id += 1;
                Node AchNode = neo.createAchieveNode(projectname,id,mainpeople,mainunit,reward,origin);
                //添加Org与Achieve依赖

                //创建org节点
                List<String> orgname = Arrays.asList(mainunit.split(";|,|；|，|、"));
                for (String on:orgname) {
                    ArrayList<Node> org = neo.search(ToNeo4j.NodeType.Organization,"name",on);
                    if (org.isEmpty()){
                        orgid += 1;
                        Node orgnode = neo.createOrgNode(on,orgid,"");
                        neo.createRel(orgnode,AchNode);
                    }
                    else {
                        for (Node orgnn:org){
                            neo.createRel(orgnn,AchNode);
                        }
                    }
                }

                //添加people依赖
                List<String> peoplename = Arrays.asList(mainpeople.split(";|,|；|，|、"));
                for (String pn:peoplename){
                    ArrayList<Node> authors = neo.search(ToNeo4j.NodeType.Author,"name",pn);
                    if (!authors.isEmpty()){
                        for (Node nn : authors){
                            String orgcheck = (String) neo.GetPropertyByName(nn,"organization");
                            if (orgname.contains(orgcheck)){
                                neo.createRel(nn,AchNode);
                            }
                        }
                    }
                }
            }
            else {
                boolean flag = true;
                for (Node insnn:projectnodes){
                    String mainpeoplecheck = (String) neo.GetPropertyByName(insnn,"mainpeople");
                    if (mainpeoplecheck == null && mainpeople == null){
                        flag = false;
                        break;
                    }
                    else if (mainpeoplecheck.equals(mainpeople)){
                        flag = false;
                        break;
                    }
                }
                if (flag == true){
                    id += 1;
                    Node AchNode = neo.createAchieveNode(projectname,id,mainpeople,mainunit,reward,origin);
                    //添加Org与Achieve依赖

                    //创建org节点
                    List<String> orgname = Arrays.asList(mainunit.split(";|,|；|，|、"));
                    for (String on:orgname) {
                        ArrayList<Node> org = neo.search(ToNeo4j.NodeType.Organization,"name",on);
                        if (org.isEmpty()){
                            orgid += 1;
                            Node orgnode = neo.createOrgNode(on,orgid,"");
                            neo.createRel(orgnode,AchNode);
                        }
                        else {
                            for (Node orgnn:org){
                                neo.createRel(orgnn,AchNode);
                            }
                        }
                    }

                    //添加people依赖
                    List<String> peoplename = Arrays.asList(mainpeople.split(";|,|；|，|、"));
                    for (String pn:peoplename){
                        ArrayList<Node> authors = neo.search(ToNeo4j.NodeType.Author,"name",pn);
                        if (!authors.isEmpty()){
                            for (Node nn : authors){
                                String orgcheck = (String) neo.GetPropertyByName(nn,"organization");
                                if (orgname.contains(orgcheck)){
                                    neo.createRel(nn,AchNode);
                                }
                            }
                        }
                    }
                }
            }

        }
        logger.info("********Achievement表数据转移完成********");
    }

}
