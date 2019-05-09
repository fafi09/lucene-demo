package com.bingo.backstage;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

public class PhraseQueryTest {  
    public static void main(String[] args) throws IOException {  
        Directory dir = new RAMDirectory();  
        Analyzer analyzer = new StandardAnalyzer();  
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);  
        iwc.setOpenMode(OpenMode.CREATE);  
        IndexWriter writer = new IndexWriter(dir, iwc);  
  
        FieldType type = new FieldType();
        type.setTokenized(false);
        type.setStored(true);
        type.setOmitNorms(true);
        type.setIndexOptions(IndexOptions.DOCS);
        
        Document doc = new Document();  
        doc.add(new TextField("text", "quick brown fox", Field.Store.YES));  
        doc.add(new Field("name", "zhangsan", type));
        writer.addDocument(doc);  
          
        doc = new Document();  
        doc.add(new TextField("text", "jumps over lazy broun dog", Field.Store.YES));  
        doc.add(new Field("name", "lisi", type));
        writer.addDocument(doc);  
          
        doc = new Document();  
        doc.add(new TextField("text", "jumps over extremely very lazy broxn dog", Field.Store.YES));  
        doc.add(new Field("name", "jhon", type));
        writer.addDocument(doc);  
          
          
        writer.close();  
  
        IndexReader reader = DirectoryReader.open(dir);  
        IndexSearcher searcher = new IndexSearcher(reader);  
          
        String term1 = "dog";  
        String term2 = "jumps";  
        PhraseQuery.Builder builder = new PhraseQuery.Builder();  
        builder.add(new Term("text",term1));  
        builder.add(new Term("text",term2));  
       //slop就是从一个词到另一个词的最大距离
        builder.setSlop(8);  
        PhraseQuery phraseQuery = builder.build();
        TopDocs results = searcher.search(phraseQuery, 100);  
        ScoreDoc[] scoreDocs = results.scoreDocs;  
          
        for (int i = 0; i < scoreDocs.length; ++i) {  
            //System.out.println(searcher.explain(query, scoreDocs[i].doc));  
            int docID = scoreDocs[i].doc;  
            Document document = searcher.doc(docID);  
            String path = document.get("text");  
            System.out.println("text:" + path);  
        }  
        System.out.println("------------------------");
        FuzzyQuery query = new FuzzyQuery(new Term("name","lisi")); 
        TopDocs tds = searcher.search(query, 10);
        System.out.println("一共查询了:"+tds.totalHits);
        for(ScoreDoc sd:tds.scoreDocs) {
            Document doc1 = searcher.doc(sd.doc);
            System.out.println(doc1.get("text")+"--"+doc1.get("name"));
        }
    }  
}  
