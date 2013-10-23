package edu.cmu.lti.f13.hw4.hw4_xiaohuay.annotators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.uima.analysis_component.JCasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.FSIterator;
import org.uimafit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.FSList;
import org.apache.uima.jcas.tcas.Annotation;

import edu.cmu.lti.f13.hw4.hw4_xiaohuay.utils.Utils;
import edu.cmu.lti.f13.hw4.hw4_xiaohuay.typesystems.Document;
import edu.cmu.lti.f13.hw4.hw4_xiaohuay.typesystems.Token;

public class DocumentVectorAnnotator extends JCasAnnotator_ImplBase {

  private Pattern tokenPattern = Pattern.compile("[\\w\'-]+|$*\\d+\\.\\d+");

  @Override
  public void process(JCas jcas) throws AnalysisEngineProcessException {

    FSIterator<Annotation> iter = jcas.getAnnotationIndex().iterator();
    if (iter.isValid()) {
      iter.moveToNext();
      Document doc = (Document) iter.get();
      createTermFreqVector(jcas, doc);
    }

  }

  /**
   * 
   * @param jcas
   *          The CAS object that stores the annotation results
   * @param doc
   *          The input document for annotation
   */

  private void createTermFreqVector(JCas jcas, Document doc) {
    // construct a vector of tokens and update the tokenList in CAS

    String docText = doc.getText();
    Matcher matcher = tokenPattern.matcher(docText);
    // annotates each token in the document
    while (matcher.find()) {
      Token token = new Token(jcas);
      token.setText(matcher.group(0));
      token.addToIndexes();
    }

    Map<String, Integer> termFreqMap = new HashMap<String, Integer>();
    ArrayList<Token> tokenList = new ArrayList<Token>();
    for (Token token : JCasUtil.selectCovered(Token.class, doc)) {
      String tokenText = token.getText();
      if (tokenText != null && Pattern.matches("\\p{Punct}", tokenText)) {
        continue;
      }
      if (termFreqMap.containsKey(tokenText)) {
        termFreqMap.put(tokenText, termFreqMap.get(tokenText) + 1);
      } else {
        termFreqMap.put(tokenText, 1);
      }
    }
    for (Entry<String, Integer> termEntry : termFreqMap.entrySet()) {
      Token term = new Token(jcas);
      term.setText(termEntry.getKey());
      term.setFrequency(termEntry.getValue());
      tokenList.add(term);
    }

    FSList fsTokenList = Utils.fromCollectionToFSList(jcas, tokenList);
    doc.setTokenList(fsTokenList);
  }
}
