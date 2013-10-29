package edu.cmu.lti.f13.hw4.hw4_xiaohuay.casconsumers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

  /** set of query id **/
  public HashSet<Integer> qIdSet;

  /** list of term frequency maps **/
  public List<Map<String, Integer>> wordDictList;

  /** list document indexes **/
  public List<String> docTextList;

  /** map from document index to relevance value **/
  public Map<Integer, Integer> relValueMap;

  /** map from query ID to query document index **/
  public Map<Integer, Integer> queryDocIndexMap;

  /** map from query ID to retrieved document indexes **/
  public Map<Integer, List<Integer>> retrievedDocIndexMap;

  /** map from query ID to cosine scores of corresponding documents **/
  public Map<Integer, Map<Integer, Double>> cosineScoresMap;

  /** index of the document **/
  public int docIndex;

  public void initialize() throws ResourceInitializationException {
    qIdSet = new HashSet<Integer>();
    queryDocIndexMap = new HashMap<Integer, Integer>();
    docTextList = new ArrayList<String>();
    retrievedDocIndexMap = new HashMap<Integer, List<Integer>>();
    cosineScoresMap = new HashMap<Integer, Map<Integer, Double>>();
    relValueMap = new HashMap<Integer, Integer>();
    wordDictList = new ArrayList<Map<String, Integer>>();
    docIndex = 0;
  }

  @Override
  public void processCas(CAS aCas) throws ResourceProcessException {

    JCas jcas;
    try {
      jcas = aCas.getJCas();
    } catch (CASException e) {
      throw new ResourceProcessException(e);
    }

    FSIterator<Annotation> it = jcas.getAnnotationIndex(Document.type).iterator();
    if (it.hasNext()) {
      Document doc = (Document) it.next();
      // Make sure that your previous annotators have populated this in CAS
      FSList fsTokenList = doc.getTokenList();
      // Store all the relevant indexes and values in HashMaps for further retrieval
      Map<String, Integer> termFreqMap = getTermFreqMap(fsTokenList);
      wordDictList.add(termFreqMap);
      docTextList.add(doc.getText());
      relValueMap.put(docIndex, doc.getRelevanceValue());
      qIdSet.add(doc.getQueryID());
      int relValue = doc.getRelevanceValue();
      int queryId = doc.getQueryID();
      if (relValue == 99) {
        queryDocIndexMap.put(queryId, docIndex);
      } else {
        if (retrievedDocIndexMap.containsKey(queryId)) {
          retrievedDocIndexMap.get(queryId).add(docIndex);
        } else {
          retrievedDocIndexMap.put(queryId, new ArrayList<Integer>(Arrays.asList(docIndex)));
        }
      }
      docIndex++;
    }
  }

  @Override
  public void collectionProcessComplete(ProcessTrace arg0) throws ResourceProcessException,
          IOException {

    super.collectionProcessComplete(arg0);
    List<Integer> rankList = new ArrayList<Integer>();
    for (int queryId : qIdSet) {
      // compute the cosine similarity of retrieved sentences
      Map<String, Integer> queryTermFreqMap = wordDictList.get(queryDocIndexMap.get(queryId));
      List<Integer> retrievedDocIndexes = retrievedDocIndexMap.get(queryId);
      if (!cosineScoresMap.containsKey(queryId)) {
        cosineScoresMap.put(queryId, new HashMap<Integer, Double>());
      }
      for (int retrievedDocIndex : retrievedDocIndexes) {
        Map<String, Integer> answerTermFreqMap = wordDictList.get(retrievedDocIndex);
        double cosineSim = compute_cosine(queryTermFreqMap, answerTermFreqMap);
        cosineScoresMap.get(queryId).put(retrievedDocIndex, cosineSim);
      }
      // compute the rank of retrieved sentences
      final Map<Integer, Double> similarityScoresMap = cosineScoresMap.get(queryId);
      Collections.sort(retrievedDocIndexes, new Comparator<Integer>() {
        public int compare(Integer index1, Integer index2) {
          if (similarityScoresMap.get(index1) > similarityScoresMap.get(index2)) {
            return -1;
          } else if ((similarityScoresMap.get(index1) < similarityScoresMap.get(index2))) {
            return 1;
          } else {
            return relValueMap.get(index1) > relValueMap.get(index2) ? -1 : 1;
          }
        }
      });
      int rank = 0;
      String docText = null;
      double bestScore = 0;
      for (int i = 0; i < retrievedDocIndexes.size(); i++) {
        int targetDocIndex = retrievedDocIndexes.get(i);
        if (relValueMap.get(targetDocIndex) == 1) {
          rank = i + 1;
          docText = docTextList.get(targetDocIndex);
          bestScore = similarityScoresMap.get(targetDocIndex);
          break;
        } else {
          continue;
        }
      }
      rankList.add(rank);
      System.out.println(String.format("Score: %f rank = %d rel = 1 qid = %d %s", bestScore, rank,
              queryId, docText));
    }
    // compute the metric:: mean reciprocal rank
    double metric_mrr = compute_mrr(rankList);
    System.out.println(" (MRR) Mean Reciprocal Rank ::" + metric_mrr);
  }

  /**
   * Get the (word, frequency) map of a token list
   * 
   * @param fsTokenList
   * @param clazz
   * @return The (word, frequency) map for a token list
   */

  private Map<String, Integer> getTermFreqMap(FSList fsTokenList) {
    Map<String, Integer> resultMap = new HashMap<String, Integer>();
    ArrayList<Token> tokenList = Utils.fromFSListToCollection(fsTokenList, Token.class);
    for (Token token : tokenList) {
      resultMap.put(token.getText(), token.getFrequency());
    }
    return resultMap;
  }

  /**
   * Get the cosine similarity between two bag of words
   * 
   * @param bag1
   * @param bag2
   * @return The cosine similarity between two bags of words
   * 
   */
  private double compute_cosine(Map<String, Integer> bag1, Map<String, Integer> bag2) {
    if (bag1.isEmpty() || bag2.isEmpty()) {
      return 0;
    }
    double score = 0.0;
    for (Entry<String, Integer> tokenEntry : bag1.entrySet()) {
      String tokenString = tokenEntry.getKey();
      Integer count = tokenEntry.getValue();
      if (bag2.containsKey(tokenString)) {
        score += bag2.get(tokenString) * count;
      }
    }
    return score / Math.sqrt(getLength(bag1) * getLength(bag2));
  }

  /**
   * Get the Euclidean length of a bag of word
   * 
   * @param bag
   * @return The Euclidean length computed from the (word, frequency) map
   * 
   */
  private double getLength(Map<String, Integer> bag) {
    double result = 0;
    for (Entry<String, Integer> tokenEntry : bag.entrySet()) {
      Integer count = tokenEntry.getValue();
      result += count * count;
    }
    return result;
  }

  /**
   * 
   * @param rankList
   * @return mrr
   */
  private double compute_mrr(List<Integer> rankList) {
    double metric_mrr = 0.0;
    for (int rank : rankList) {
      metric_mrr += 1.0 / rank;
    }
    metric_mrr /= rankList.size();
    return metric_mrr;
  }

}
