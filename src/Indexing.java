import java.io.*;
import java.io.ObjectInputStream.GetField;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.LucenePackage;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.FSDirectory;
public class Indexing {
	@SuppressWarnings("unchecked")
	public static void main(String args[]) throws IOException
	{
		FileSystem fs = FileSystems.getDefault();
		Path path1 = fs.getPath(args[0]);
		List<String> token=new ArrayList<String>();
		Map<String,LinkedList<Integer>> dictionary=new HashMap<>();
		IndexReader reader = DirectoryReader.open(FSDirectory.open(path1));
		Collection<String> a=MultiFields.getIndexedFields(reader);//Reading all the fields 
		Iterator<String> itr=a.iterator();//Iterator to iterate through all the fields
		int count=0,index=0,id=0;
		while(itr.hasNext())
		{
			count++;
			if(count==11)//To remove fields docId and versions
			{	 
				itr.next();
				itr.next();
			}	 

			String field=itr.next();
			Terms terms=MultiFields.getTerms(reader,field);//Reading all the terms from a given field
			TermsEnum termsEnum=terms.iterator();//Iterating through each term in the field
			while(termsEnum.next()!=null)
			{
				String tokens=termsEnum.term().utf8ToString();//Obtaining the term
				//token.add(index++,tokens);
				PostingsEnum postingEnum=MultiFields.getTermDocsEnum(reader,field,termsEnum.term());//Obtaining the posting for the term
				while(postingEnum.nextDoc()!=postingEnum.NO_MORE_DOCS)//Iterating through the postings of the term
				{ 
					id=postingEnum.docID();//Obtaninig the docId for the term
					if(dictionary.containsKey(tokens))
					{
						LinkedList<Integer> Invertedindex=dictionary.get(tokens);
						if(!Invertedindex.contains(id))//Appending new docId to the inverted index
						{
							Invertedindex.add(id);
							dictionary.put(tokens,Invertedindex);
						}		
					}
					else
					{
						LinkedList<Integer> addNewIndex=new LinkedList<>();//Adding a new term and the id
						addNewIndex.add(id);
						dictionary.put(tokens,addNewIndex);
					}
				}


			}
		}

		try
		{
			Reader readerOfFile=new InputStreamReader(new FileInputStream(args[2]),"utf-8");//reading the input file
			BufferedReader inputFile=new BufferedReader(readerOfFile);
			PrintWriter writer=new PrintWriter(new OutputStreamWriter(new FileOutputStream(args[1]),StandardCharsets.UTF_8),true);//Output File
			skip(readerOfFile);
			String query=null;
			while((query=inputFile.readLine())!=null)//Reading the data from the file until EOF
			{
				LinkedList<Integer> intermediateListAnd=new LinkedList<>();
				LinkedList<Integer> Or=new LinkedList<>();
				String[] arr=query.split(" ");
				int comparison=0,comparisonTaatOr=0,flag=0;
				for(String queryTerm : arr)
				{ 	
					
					if(dictionary.containsKey(queryTerm))
					{
						LinkedList<Integer> getPostingsList=dictionary.get(queryTerm);//Getting the posting list for the term
						writer.append("GetPostings\n");
						writer.append(queryTerm+"\n");
						writer.append("Postings list: ");
						int postingListIndex=0;
						while(getPostingsList.size()>postingListIndex)
						{
							writer.append(getPostingsList.get(postingListIndex++)+" ");//Appending each docId for the term to a output file
						}
						writer.append("\n");
					}
					//TAAT AND
					if(dictionary.containsKey(queryTerm))
					{
						if(intermediateListAnd.isEmpty() && flag==0)//If the intermediate list in empty assign it a linked list of a query term
						{	
							intermediateListAnd=(LinkedList<Integer>) dictionary.get(queryTerm).clone();
							flag=1;
						}	
						else
						{
							LinkedList<Integer> termList=new LinkedList<>();
							termList=(LinkedList<Integer>) dictionary.get(queryTerm).clone();
							int termListIndex=0,intermediateListIndex=0;
							while(termListIndex<termList.size() && intermediateListIndex<intermediateListAnd.size())//Iterating through the current term linked list and intermediate linked list
							{
								if(termList.get(termListIndex)==intermediateListAnd.get(intermediateListIndex))//If matching id is found
								{
									termListIndex++;
									intermediateListIndex++;
									comparison++;
								}
								else
								{
									if(termList.get(termListIndex)>intermediateListAnd.get(intermediateListIndex))//If current term list id is greater then the intermediate list
									{
										intermediateListAnd.remove(intermediateListIndex);
										comparison++;
									}
									else//If intermediate list id is greater than that of the current term
									{
										termListIndex++;
										comparison++;
									}
								}

							}
							while(intermediateListIndex<intermediateListAnd.size())//If intermediate term list size is greater than the size of the current term list
							{
								intermediateListAnd.remove(intermediateListIndex);
							}
						}
					}
					//Taat OR
					if(dictionary.containsKey(queryTerm))
					{
						if(Or.isEmpty())//If the intermediate list in empty assign it a linked list of a query term
						{	
							Or=(LinkedList<Integer>) dictionary.get(queryTerm).clone();
						}
						else
						{
							LinkedList<Integer> termList=new LinkedList<>();
							termList=(LinkedList<Integer>) dictionary.get(queryTerm).clone();
							int termListIndexOr=0,intermediateListIndexOr=0;
							while(termListIndexOr<termList.size() && intermediateListIndexOr<Or.size())//Iterating through the current term linked list and intermediate linked list
							{
								if(termList.get(termListIndexOr)==Or.get(intermediateListIndexOr))//If matching id is found
								{
									intermediateListIndexOr++;
									termListIndexOr++;
									comparisonTaatOr++;
								}
								else
								{
									if(termList.get(termListIndexOr)>Or.get(intermediateListIndexOr))//If current term list id is greater then the intermediate list
									{
										intermediateListIndexOr++;
										comparisonTaatOr++;
									}
									else
									{
										Or.add(intermediateListIndexOr,termList.get(termListIndexOr));//If intermediate list id is greater than that of the current term
										termListIndexOr++;
										comparisonTaatOr++;
									}
								}
							}
							if(termListIndexOr<termList.size())////If intermediate list size is lesser than the size of the current term list
							{
								while(termListIndexOr<termList.size())
									Or.add(termList.get(termListIndexOr++));

							}

						}
					}
				}
				Iterator it = dictionary.entrySet().iterator();
				Or.sort(new Comparator<Integer>() {

					@Override
					public int compare(Integer o1, Integer o2) {
						// TODO Auto-generated method stub
						if(o1 < o2)
							return -1;
						else
							return 1;
					}

				});
				Iterator printIterator=intermediateListAnd.iterator();
				writer.append("TaatAnd\n");
				writer.append(query+"\n");
				writer.append("Results: ");
				if(printIterator.hasNext())
				{	
					while(printIterator.hasNext())
					{	
						writer.append(printIterator.next().toString()+" ");
					}
				}
				else
				{
					writer.append("empty");
				}
				writer.append("\nNumber of documents in results: "+intermediateListAnd.size());
				writer.append("\nNumber of comparisons: "+comparison+"\n");
				
				printIterator=Or.iterator();
				writer.append("TaatOr\n");
				writer.append(query+"\n");
				writer.append("Results: ");
				if(printIterator.hasNext())
				{	
					while(printIterator.hasNext())
					{
						writer.append(printIterator.next().toString()+" ");
					}
				}
				else
				{
					writer.append("empty");
					
				}
				writer.append("\nNumber of documents in results: "+Or.size());
				writer.append("\nNumber of comparisons: "+comparisonTaatOr+"\n");
				//DAAT
				int countTerms=0,DaatOrStartListIndex=0,DaatOrStartListIndexSize=0;
				ArrayList<LinkedList<Integer>> termsList = new ArrayList<LinkedList<Integer>>();
				LinkedList<Integer> DaatAnd=new LinkedList<>();
				for(int i=0;i<arr.length;i++)
				{ 	 
					String queryTerm=arr[i];
					LinkedList<Integer> postingsList=new LinkedList<>();
					postingsList=(LinkedList<Integer>) dictionary.get(queryTerm).clone();
					termsList.add(postingsList);
					if(DaatOrStartListIndexSize<postingsList.size())//Find the index of the linked list with the largest size in our arrayList
					{	
						DaatOrStartListIndex=termsList.indexOf(postingsList);
						DaatOrStartListIndexSize=postingsList.size();
					}
					countTerms++;//Counting the number of query terms
				}
				int comparisonDaat=0,comparisonDaatOr=0;
				flag=0;
				for(int i=0;i<termsList.get(0).size();i++)//Obtaining the docId of the first linked list
				{
					if(termsList.size()>1)//If more than one words in the query
					{
						int TotalNumberOfDocsWithTerms=0;
						for(int j=1;j<countTerms;j++)//Iterating through all the other linked lists
						{
							LinkedList<Integer> postingsList=new LinkedList<>();
							postingsList=(LinkedList<Integer>)termsList.get(j).clone();
							int Daatindex=0;
							while(postingsList.size()>Daatindex)//Iterating through a linked list at index j
							{
								if(postingsList.get(Daatindex)==termsList.get(0).get(i))//If matching docId 
								{
									comparisonDaat++;
									TotalNumberOfDocsWithTerms++;
									break;
								}
								else
								{
									if(postingsList.get(Daatindex)>termsList.get(0).get(i))//if docId not found i.e id of current link list is greater than that we are finding
									{
										flag=1;
										comparisonDaat++;
										break;
									}
									else
									{
										if(!DaatAnd.isEmpty() && postingsList.get(Daatindex)>DaatAnd.getLast())
										{
											comparisonDaat++;
											Daatindex++;

										}
										else
										{
											if(DaatAnd.isEmpty())
												comparisonDaat++;
											Daatindex++;
										}
									}
								}
							}
							if(flag==1)
								break;
						}
						if(TotalNumberOfDocsWithTerms==countTerms-1)
						{		
							DaatAnd.add(termsList.get(0).get(i));
						}
					}
				}
				printIterator=DaatAnd.iterator();
				writer.append("DaatAnd\n");
				writer.append(query+"\n");
				writer.append("Results: ");
				if(printIterator.hasNext())
				{	
					while(printIterator.hasNext())
					{
						writer.append(printIterator.next().toString()+" ");
					}
				}
				else
				{
					writer.append("empty");
				}
				writer.append("\nNumber of documents in results: "+DaatAnd.size());
				writer.append("\nNumber of comparisons: "+comparisonDaat+"\n");
				//Daat Or
				Map<Integer,Integer> DaatOr=new HashMap<>();
				for(int i=0;i<termsList.get(DaatOrStartListIndex).size();i++)//iterating through the linked list with largest size
				{
					if(termsList.size()>1)
					{
						for(int j=0;j<countTerms;j++)//Iterating through all the other linked list
						{
							LinkedList<Integer> postingsList=new LinkedList<>();
							postingsList=(LinkedList<Integer>)termsList.get(j).clone();
							if(postingsList.size()>i)//If posting list obtained size is greater than the size of the current index being visited
							{	
								DaatOr.put(postingsList.get(i),1);//Add id to the map
								comparisonDaatOr++;
							}
						}
					}
				}
				writer.append("DaatOr\n");
				writer.append(query+"\n");
				writer.append("Results: ");
				Map ascSortedMap = new TreeMap();//Sorting the map
				ascSortedMap.putAll(DaatOr);
				Iterator daatOr = ascSortedMap.entrySet().iterator();
				if(daatOr.hasNext())
				{		
					while (daatOr.hasNext()) 
					{
						Map.Entry pair = (Map.Entry)daatOr.next();
						writer.append(pair.getKey()+" ");
					}
				}
				else
				{
					writer.append("empty");
				}
				writer.append("\nNumber of documents in results: "+DaatOr.size());
				writer.append("\nNumber of comparisons: "+comparisonDaatOr+"\n");
			}

			inputFile.close();
			writer.close();
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	public static void skip(Reader reader) throws IOException
	{
		//reader.mark(1);
		char[] possibleBOM = new char[1];
		reader.read(possibleBOM);

		if (possibleBOM[0] != '\ufeff')
		{
			reader.reset();
		}
	}
}
