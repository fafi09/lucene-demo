package com.bingo.backstage;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;

/**
 * Created by MoSon on 2017/6/30.
 */
public class SearchFiles {
	private SearchFiles() {
	}

	public static void main(String[] args) throws Exception {
		String usage = "Usage:\tjava com.bingo.backstage.SearchFiles [-index dir] [-field f] [-repeat n] [-queries file] [-query string] [-raw] [-paging hitsPerPage]\n\nSee http://lucene.apache.org/core/4_1_0/demo/ for details.";
		if (args.length > 0 && ("-h".equals(args[0]) || "-help".equals(args[0]))) {
			System.out.println(usage);
			System.exit(0);
		}

		String index = "index";
		String field = "contents";
		String queries = null;
		int repeat = 0;
		boolean raw = false;
		String queryString = null;
		int hitsPerPage = 10;

		for (int reader = 0; reader < args.length; ++reader) {
			if ("-index".equals(args[reader])) {
				index = args[reader + 1];
				++reader;
			} else if ("-field".equals(args[reader])) {
				field = args[reader + 1];
				++reader;
			} else if ("-queries".equals(args[reader])) {
				queries = args[reader + 1];
				++reader;
			} else if ("-query".equals(args[reader])) {
				queryString = args[reader + 1];
				++reader;
			} else if ("-repeat".equals(args[reader])) {
				repeat = Integer.parseInt(args[reader + 1]);
				++reader;
			} else if ("-raw".equals(args[reader])) {
				raw = true;
			} else if ("-paging".equals(args[reader])) {
				hitsPerPage = Integer.parseInt(args[reader + 1]);
				if (hitsPerPage <= 0) {
					System.err.println("There must be at least 1 hit per page.");
					System.exit(1);
				}

				++reader;
			}
		}

		// 打开文件
		DirectoryReader var18 = DirectoryReader.open(FSDirectory.open(Paths.get(index, new String[0])));
		IndexSearcher searcher = new IndexSearcher(var18);
		StandardAnalyzer analyzer = new StandardAnalyzer();
		BufferedReader in = null;
		if (queries != null) {
			in = Files.newBufferedReader(Paths.get(queries, new String[0]), StandardCharsets.UTF_8);
		} else {
			in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
		}

		QueryParser parser = new QueryParser(field, analyzer);

		do {
			if (queries == null && queryString == null) {
				System.out.println("Enter query: ");
			}

			String line = queryString != null ? queryString : in.readLine();
			if (line == null || line.length() == -1) {
				break;
			}

			line = line.trim();
			if (line.length() == 0) {
				break;
			}

			Query query = parser.parse(line);
			System.out.println("Searching for: " + query.toString(field));
			if (repeat > 0) {
				Date start = new Date();

				for (int end = 0; end < repeat; ++end) {
					searcher.search(query, 100);
				}

				Date var19 = new Date();
				System.out.println("Time: " + (var19.getTime() - start.getTime()) + "ms");
			}

			doPagingSearch(in, searcher, query, hitsPerPage, raw, queries == null && queryString == null);
		} while (queryString == null);

		var18.close();
	}

	public static void doPagingSearch(BufferedReader in, IndexSearcher searcher, Query query, int hitsPerPage,
			boolean raw, boolean interactive) throws IOException {
		TopDocs results = searcher.search(query, 5 * hitsPerPage);
		ScoreDoc[] hits = results.scoreDocs;
		int numTotalHits = results.totalHits;
		System.out.println(numTotalHits + " total matching documents");
		int start = 0;
		int end = Math.min(numTotalHits, hitsPerPage);

		while (true) {
			if (end > hits.length) {
				System.out.println("Only results 1 - " + hits.length + " of " + numTotalHits
						+ " total matching documents collected.");
				System.out.println("Collect more (y/n) ");
				String quit = in.readLine();
				if (quit.length() == 0 || quit.charAt(0) == 110) {
					break;
				}

				hits = searcher.search(query, numTotalHits).scoreDocs;
			}

			end = Math.min(hits.length, start + hitsPerPage);

			for (int var15 = start; var15 < end; ++var15) {
				if (raw) {
					System.out.println("doc=" + hits[var15].doc + " score=" + hits[var15].score);
				} else {
					Document line = searcher.doc(hits[var15].doc);
					String page = line.get("path");
					if (page != null) {
						System.out.println(var15 + 1 + ". " + page);
						String title = line.get("title");
						if (title != null) {
							System.out.println(" Title: " + line.get("title"));
						}
						String contents = line.get("contents");
						if (contents != null) {
							System.out.println(" contents: " + line.get("contents"));
						}
					} else {
						System.out.println(var15 + 1 + ". No path for this document");
					}
				}
			}

			if (!interactive || end == 0) {
				break;
			}

			if (numTotalHits >= end) {
				boolean var16 = false;

				while (true) {
					System.out.print("Press ");
					if (start - hitsPerPage >= 0) {
						System.out.print("(p)revious page, ");
					}

					if (start + hitsPerPage < numTotalHits) {
						System.out.print("(n)ext page, ");
					}

					System.out.println("(q)uit or enter number to jump to a page.");
					String var17 = in.readLine();
					if (var17.length() == 0 || var17.charAt(0) == 113) {
						var16 = true;
						break;
					}

					if (var17.charAt(0) == 112) {
						start = Math.max(0, start - hitsPerPage);
						break;
					}

					if (var17.charAt(0) == 110) {
						if (start + hitsPerPage < numTotalHits) {
							start += hitsPerPage;
						}
						break;
					}

					int var18 = Integer.parseInt(var17);
					if ((var18 - 1) * hitsPerPage < numTotalHits) {
						start = (var18 - 1) * hitsPerPage;
						break;
					}

					System.out.println("No such page");
				}

				if (var16) {
					break;
				}

				end = Math.min(numTotalHits, start + hitsPerPage);
			}
		}

	}
}