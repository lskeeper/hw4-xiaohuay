package edu.cmu.lti.f13.hw4.hw4_xiaohuay.casconsumers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.collection.CasConsumer_ImplBase;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.FSList;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceProcessException;
import org.apache.uima.util.ProcessTrace;

import edu.cmu.lti.f13.hw4.hw4_xiaohuay.typesystems.Document;
import edu.cmu.lti.f13.hw4.hw4_xiaohuay.typesystems.Token;
import edu.cmu.lti.f13.hw4.hw4_xiaohuay.utils.Utils;

public class RetrievalEvaluator extends CasConsumer_ImplBase {

  /** query id number **/
  public ArrayList<Integer> qIdList;

  /** query and text relevant values **/
  public ArrayList<Integer> relList;

  public List<Map<String, Integer>> wordDictList;

  public ArrayList<String> globalWordDictionary;

  public Map<Integer, Integer> queryDocIndexMap;

  public Map<Integer, List<Integer>> retreivalDocIndexMap;

  public void initialize() throws ResourceInitializationException {

    qIdList = new ArrayList<Integer>();
    relList = new ArrayList<Integer>();
    queryDocIndexMap = new HashMap<Integer, Integer>();
    retreivalDocIndexMap = new HashMap<Integer, List<Integer>>();
  }

  /**
   * TODO :: 1. construct the global word dictionary 2. keep the word frequency for each sentence
   */
  @Override
  public void processCas(CAS aCas) throws ResourceProcessException {

    JCas jcas;
    try {
      jcas = aCas.getJCas();
    } catch (CASException e) {
      throw new ResourceProcessException(e);
    }

    FSIterator<Annotation> it = jcas.getAnnotationIndex(Document.type).iterator();
    int docIndex = 0;
    while (it.hasNext()) {
      Document doc = (Document) it.next();
      // Make sure that your previous annotators have populated this in CAS
      FSList fsTokenList = doc.getTokenList();
      Map<String, Integer> termFreqMap = getTermFreqMap(fsTokenList);
      globalWordDictionary.addAll(termFreqMap.keySet());
      wordDictList.add(termFreqMap);
      qIdList.add(doc.getQueryID());
      int relValue = doc.getRelevanceValue();
      int queryId = doc.getQueryID();
      relList.add(relValue);
      if (relValue == 99) {
        queryDocIndexMap.put(queryId, docIndex);
      } else {
        if (retreivalDocIndexMap.containsKey(queryId)) {
          retreivalDocIndexMap.get(queryId).add(docIndex);
        } else {
          retreivalDocIndexMap.put(queryId, new ArrayList<Integer>(Arrays.asList(docIndex)));
        }
        
      }
    }
    for (Map<String, Integer> termFreqMap : wordDictList) {
      Set<String> keySet = termFreqMap.keySet();
      for (String word : globalWordDictionary) {
        if (!keySet.contains(word)) {
          termFreqMap.put(word, 0);
        }
      }
    }
  }

  private Map<String, Integer> getTermFreqMap(FSList fsTokenList) {
    Map<String, Integer> resultMap = new HashMap<String, Integer>();
    ArrayList<Token> tokenList = Utils.fromFSListToCollection(fsTokenList, Token.class);
    for (Token token : tokenList) {
      resultMap.put(token.getText(), token.getFrequency());
    }
    return resultMap;
  }

  /**
   * TODO 1. Compute Cosine Similarity and rank the retrieved sentences 2. Compute the MRR metric
   */
  @Override
  public void collectionProcessComplete(ProcessTrace arg0) throws ResourceProcessException,
          IOException {

    super.collectionProcessComplete(arg0);

    // TODO :: compute the cosine similarity measure
    double cosineSim = 0;
    int docNum = qIdList.size();
    int currentQueryId = 0;
    for (int queryId : qIdList) {
      if (queryId != currentQueryId) {
        currentQueryId = queryId;
        Map<String, Integer> queryTermFreqMap = new HashMap<String, Integer>();
        for (int i = 0; i < docNum; i++) {
          if (qIdList.get(i) == currentQueryId) {
            if (relList.get(i) == 99) {
              queryTermFreqMap = wordDictList.get(i);
            } else {
              if (relList.get(i) == 0) {
              }
            }
          }
        }

      }
    }

    // TODO :: compute the rank of retrieved sentences

    // TODO :: compute the metric:: mean reciprocal rank
    double metric_mrr = compute_mrr();
    System.out.println(" (MRR) Mean Reciprocal Rank ::" + metric_mrr);
  }

  /**
   * 
   * @return cosine_similarity
   */
  private double computeCosineSimilarity(Map<String, Integer> queryVector,
          Map<String, Integer> docVector) {
    double cosine_similarity = 0.0;

    // TODO :: compute cosine similarity between two sentences

    return cosine_similarity;
  }

  /**
   * 
   * @return mrr
   */
  private double compute_mrr() {
    double metric_mrr = 0.0;

    // TODO :: compute Mean Reciprocal Rank (MRR) of the text collection

    return metric_mrr;
  }

}
