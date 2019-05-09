package com.bingo.backstage;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class QuerySearch {
	
	private FSDirectory directory;
	private DirectoryReader reader;
	private String[] ids = {"1","2","3","4","5","6"};
    private String[] emails = {"aa@itat.org","bb@itat.org","cc@cc.org","dd@sina.org","ee@zttc.edu","ff@itat.org"};
    private String[] contents = {
            "welcome to visited the space,I like book",
            "hello boy, I like pingpeng ball",
            "my name is cc I like game",
            "I like football",
            "I like football and I like basketball too",
            "I like movie and swim"
    };
    private int[] attachs = {2,3,1,4,5,5};
    private String[] names = {"zhangsan","lisi","john","jetty","lisi","jake"};
    
    private Map<String,Float> scores = new HashMap<String,Float>();
    
    public QuerySearch(){
        try {
            directory = FSDirectory.open(Paths.get("D://lucene//index"));
            scores.put("itat.org", 1.5f);
            scores.put("cc.org", 2.0f);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    /**
     * 创建索引
     */
    @SuppressWarnings("deprecation")
    public void index(){
        IndexWriter writer = null;
        try {
            directory = FSDirectory.open(Paths.get("D://lucene//index"));
            writer = getWriter(true);
            Document doc = null;
            FieldType type = new FieldType();
            type.setTokenized(false);
            type.setStored(true);
            /*
             * mitNorms: Norms是normalization的缩写，
             * lucene允许每个文档的每个字段都存储一个normalization factor，
             * 是和搜索时的相关性计算有关的一个系数。Norms的存储只占一个字节，
             * 但是每个文档的每个字段都会独立存储一份，且Norms数据会全部加载到内存。
             * 所以若开启了Norms，会消耗额外的存储空间和内存。但若关闭了Norms，
             * 则无法做index-time boosting（elasticsearch官方建议使用query-time boosting来替代）
             * 以及length normalization。
             */
            type.setOmitNorms(true);
            /*
             * NONE：Not indexed 不索引
             * DOCS: 反向索引中只存储了包含该词的 文档id，没有词频、位置
             * DOCS_AND_FREQS: 反向索引中会存储 文档id、词频
             * DOCS_AND_FREQS_AND_POSITIONS:反向索引中存储 文档id、词频、位置
             * DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS :反向索引中存储 文档id、词频、位置、偏移量
             * 
             */
            type.setIndexOptions(IndexOptions.DOCS);
            FieldType type1 = new FieldType();
            type1.setTokenized(true);
            type1.setStored(false);

            /*
             * field "content" was indexed without position data; cannot run PhraseQuery
             */
            type1.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            for(int i=0;i<ids.length;i++){
                doc = new Document();
                doc.add(new Field("id", ids[i], type));
                doc.add(new Field("name", names[i], type));
                doc.add(new Field("content", contents[i], type1));
                //存储数字
                doc.add(new IntPoint("attach", attachs[i]));
                
                // 加权操作
                TextField field = new TextField("email", emails[i], Field.Store.YES);
                String et = emails[i].substring(emails[i].lastIndexOf("@")+1);
                if (scores.containsKey(et)) {
                    field.setBoost(scores.get(et));
                }
                doc.add(field);
                // 添加文档
                writer.addDocument(doc);
            }
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }finally{
            try {
                writer.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
	private IndexWriter getWriter(boolean create) throws IOException {

		StandardAnalyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
		if (create) {
			iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
		} else {
			iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
		}

		IndexWriter writer = new IndexWriter(directory, iwc);
		return writer;
	}
	/**
     * getSearcher
     * @return
     */
    public IndexSearcher getSearcher(){
        try {
            directory = FSDirectory.open(Paths.get("D://lucene//index"));
            if(reader==null){
                reader = DirectoryReader.open(directory);
            }else{
                reader.close();
            }
            return new IndexSearcher(reader);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }
    
    
    /**
     * 精准匹配
     */
    public void search(String searchField,String field){
        // 得到读取索引文件的路径
        IndexReader reader = null;
        try {
            directory = FSDirectory.open(Paths.get("D://lucene//index"));
            reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);
            // 运用term来查找
            Term t = new Term(searchField, field);
            Query q = new TermQuery(t);
            // 获得查询的hits
            TopDocs hits = searcher.search(q, 10);
            // 显示结果
            System.out.println("匹配 '" + q + "'，总共查询到" + hits.totalHits + "个文档");
            for (ScoreDoc scoreDoc : hits.scoreDocs){
                Document doc = searcher.doc(scoreDoc.doc);
                System.out.println("id:"+doc.get("id")+":"+doc.get("name")+",email:"+doc.get("email"));
            }
            
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            try {
                reader.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    
    /**
     * between
     * @param field
     * @param start
     * @param end
     * @param num
     */
    public void searchByTermRange(String field,String start,String end,int num) {
        try {
            IndexSearcher searcher = getSearcher();
            BytesRef lowerTerm = new BytesRef(start.getBytes()) ;
            BytesRef upperTerm = new BytesRef(end.getBytes()) ;
            
            Query query = new TermRangeQuery(field, lowerTerm , upperTerm, true, true);
            TopDocs tds = searcher.search(query, num);
            
            System.out.println("一共查询了:"+tds.totalHits);
            for(ScoreDoc sd:tds.scoreDocs) {
                Document doc = searcher.doc(sd.doc);
                System.out.println(doc.get("id")+"---->"+
                        doc.get("name")+"["+doc.get("email")+"]-->"+doc.get("id")+","+
                        doc.get("attach"));
            }
        } catch (CorruptIndexException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 匹配其索引开始以指定的字符串的文档
     * @param field
     * @param value
     * @param num
     */
    public void searchByPrefix(String field,String value,int num) {
        try {
            IndexSearcher searcher = getSearcher();
            Query query = new PrefixQuery(new Term(field,value));
            TopDocs tds = searcher.search(query, num);
            System.out.println("一共查到："+tds.totalHits);
            for(ScoreDoc scoreDoc:tds.scoreDocs){
                Document doc = searcher.doc(scoreDoc.doc);
                System.out.println(doc.get("id")+"---->"+
                        doc.get("name")+"["+doc.get("email")+"]-->"+doc.get("id")+","+
                        doc.get("attach"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 数字搜索
     * @param field
     * @param start
     * @param end
     * @param num
     */
    public void searchByNums(String field,int start,int end,int num){
        try {
            IndexSearcher searcher = getSearcher();
            Query query =   IntPoint.newRangeQuery(field, start, end);
            TopDocs tds = searcher.search(query, num);
            System.out.println("一共查到："+tds.totalHits);
            for(ScoreDoc scoreDoc:tds.scoreDocs){
                Document doc = searcher.doc(scoreDoc.doc);
                System.out.println(doc.get("id")+"---->"+
                        doc.get("name")+"["+doc.get("email")+"]-->"+doc.get("id")+","+
                        doc.get("attach"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 通配符
     * @param field
     * @param value
     * @param num
     */
    public void searchByWildcard(String field,String value,int num){
        try {
            IndexSearcher searcher = getSearcher();
            WildcardQuery query = new WildcardQuery(new Term(field,value));
            TopDocs tds = searcher.search(query, num);
            System.out.println("一共查到："+tds.totalHits);
            for(ScoreDoc scoreDoc:tds.scoreDocs){
                Document doc = searcher.doc(scoreDoc.doc);
                System.out.println(doc.get("id")+"---->"+
                        doc.get("name")+"["+doc.get("email")+"]-->"+doc.get("id")+","+
                        doc.get("attach"));
            }
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }
    /**
     * BooleanQuery可以连接多个子查询
     * Occur.MUST表示必须出现
     * Occur.SHOULD表示可以出现
     * Occur.MUSE_NOT表示不能出现
     * 1．MUST和MUST：取得连个查询子句的交集。 
     * 2．MUST和MUST_NOT：表示查询结果中不能包含MUST_NOT所对应得查询子句的检索结果。 
     * 3．SHOULD与MUST_NOT：连用时，功能同MUST和MUST_NOT。
     * 4．SHOULD与MUST连用时，结果为MUST子句的检索结果,但是SHOULD可影响排序。
     * 5．SHOULD与SHOULD：表示“或”关系，最终检索结果为所有检索子句的并集。
     * 6．MUST_NOT和MUST_NOT：无意义，检索无结果。
     * @param field
     * @param value
     * @param num
     */
    @SuppressWarnings("deprecation")
    public void searchByBoolean(String[] field,String[] value,int num){
        try {
            if(field.length!=value.length){
                System.out.println("field的长度需要与value的长度相等！");
                System.exit(0);
            }
            IndexSearcher searcher = getSearcher();
            BooleanQuery query = null;
            TopDocs tds = null;
        	BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for(int i = 0;i<field.length;i++){
            	builder.add(new TermQuery(new Term(field[i],value[i])),BooleanClause.Occur.SHOULD);
            }
            query = builder.build();
            tds = searcher.search(query, num);
            System.out.println("一共查询:"+tds.totalHits);
            for(ScoreDoc doc:tds.scoreDocs){
                Document document = searcher.doc(doc.doc);
                System.out.println(document.get("id")+"---->"+
                        document.get("name")+"["+document.get("email")+"]-->"+document.get("id")+","+
                        document.get("attach"));
            }
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }
    public void searchByPhrase(int num){
        try {
            IndexSearcher searcher = getSearcher();
            PhraseQuery.Builder builder = new PhraseQuery.Builder();
            /*
             * field "content" was indexed without position data; cannot run PhraseQuery
             */
            builder.setSlop(3); //slop就是从一个词到另一个词的最大距离
            builder.add(new Term("content","like"));
//            //第一个Term
            builder.add(new Term("content","football"));
            
            PhraseQuery query = builder.build();
            TopDocs tds = searcher.search(query, num);
            System.out.println("一共查询了:"+tds.totalHits);
            for(ScoreDoc sd:tds.scoreDocs) {
                Document doc = searcher.doc(sd.doc);
                System.out.println(doc.get("id")+"---->"+
                        doc.get("name")+"["+doc.get("email")+"]-->"+doc.get("id")+","+
                        doc.get("attach"));
            }
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }
    /**
     * 相似度匹配查询
     * @param num
     */
    public void searchByFuzzy(int num) {
        try {
            IndexSearcher searcher = getSearcher();
            FuzzyQuery query = new FuzzyQuery(new Term("name","li")); 
            TopDocs tds = searcher.search(query, num);
            System.out.println("一共查询了:"+tds.totalHits);
            for(ScoreDoc sd:tds.scoreDocs) {
                Document doc = searcher.doc(sd.doc);
                System.out.println(doc.get("id")+"---->"+
                        doc.get("name")+"["+doc.get("email")+"]-->"+doc.get("id")+","+
                        doc.get("attach")+","+doc.get("date"));
            }
        } catch (CorruptIndexException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void searchByQueryParse(Query query,int num) {
        try {
            IndexSearcher searcher = getSearcher();
            TopDocs tds = searcher.search(query, num);
            System.out.println("一共查询了:"+tds.totalHits);
            for(ScoreDoc sd:tds.scoreDocs) {
                Document doc = searcher.doc(sd.doc);
                System.out.println(doc.get("id")+"---->"+
                        doc.get("name")+"["+doc.get("email")+"]-->"+doc.get("id")+","+
                        doc.get("attach")+","+doc.get("date")+"=="+sd.score);
            }
        } catch (CorruptIndexException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws ParseException {
		QuerySearch o = new QuerySearch();
		//o.index();
    	/*o.search("content", "football");*/
		//o.searchByTermRange("email", "aa", "ff", 3);
    	//o.searchByPrefix("content", "i", 9);  //建立索引后大写变为了小写
    	//o.searchByNums("attach", 1, 5, 2);
		//o.searchByWildcard("content", "bo*", 2);
		//o.searchByBoolean(new String[]{"content","email"}, new String[] {"game","aa"}, 6);
		//o.searchByPhrase(6);
		//o.searchByFuzzy(3);
		
		/*
		 * //6. 先分词再搜索
        //创建查询解析器
        //参数1：要查询的字段的名字，必须有才行
        //参数2：分词器，在查询的时候，先将关键字分词，再搜索
		 */
		/*QueryParser parser =new QueryParser("content", new StandardAnalyzer());
		Query query =parser.parse("like");
		o.searchByQueryParse(query, 10);*/
		
		System.out.println("=============");
		//1.无条件的查询所有文档
		Query matchAllquery =new MatchAllDocsQuery();
		//===========开始搜索
		//参数1：查询条件对象，参数2：查询的最大条数，该参数不能为空
		//返回的是：当前指定数量的所有的文档记录（具有排名效果）结果，类似一个集合
 		o.searchByQueryParse(matchAllquery, 10);
	}
}
