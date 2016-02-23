/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.mesos;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mesos.Protos;

/**
 * Boolean expression evaluator for Mesos slave attributes
 */
public class AttributeEvaluator {

  private Map<String, String> mAttrsMap = new HashMap<String, String>();

  private void setAttributes(List<Protos.Attribute> attributes) {
    if (attributes == null || attributes.isEmpty()) {
      return;
    }

    for (Protos.Attribute attr : attributes) {
      // Mesos attribute : text ":" ( scalar | range | text )
      // now only support text attributes.
      mAttrsMap.put(attr.getName(), attr.getText().getValue());
    }
  }

  /**
   * @param attributes Mesos slave attributes
   */
  public AttributeEvaluator(List<Protos.Attribute> attributes) {
    setAttributes(attributes);
  }

  /**
   * @param constraints Mesos slave attributes
   * @return return True if matched constraints
   */
  public boolean matchAttributes(String constraints) {
    Lexer lexer = new Lexer(constraints);
    Parser parser = new Parser(lexer);
    AttributeExpression expression = parser.build();
    return expression.interpret();
  }

  /**
   * Constraints Lexer
   */
  private class Lexer {
    private StreamTokenizer mInput;

    private int mSymbol = NONE;
    public static final int EOL = -3;
    public static final int EOF = -2;
    public static final int INVALID = -1;

    public static final int NONE = 0;

    public static final int OR = 1;
    public static final int AND = 2;
    public static final int NOT = 3;

    public static final int TRUE = 4;
    public static final int FALSE = 5;

    public static final int LEFT = 6;
    public static final int RIGHT = 7;

    /**
     * @param constraints schedule constraints
     */
    public Lexer(String constraints) {
      Reader r = buildReader(constraints);
      mInput = new StreamTokenizer(r);
      resetSyntax();
    }

    private void resetSyntax() {
      mInput.resetSyntax();
      mInput.wordChars('0', 'z');
      mInput.whitespaceChars('\u0000', ' ');
      mInput.whitespaceChars('\n', '\t');

      mInput.ordinaryChar('(');
      mInput.ordinaryChar(')');
      mInput.ordinaryChar('&');
      mInput.ordinaryChar('|');
      mInput.ordinaryChar('!');
    }

    private Reader buildReader(String arg) {
      ByteArrayInputStream byteStream = new ByteArrayInputStream(arg.getBytes());
      return new BufferedReader(new InputStreamReader(byteStream));
    }

    private boolean matchAttributes(String val) {
      if (val == null) {
        return false;
      }

      if (!val.contains(":")) {
        return false;
      }

      String[] kv = val.split(":");
      String attr = kv[0];
      String text = kv[1];

      return text.equals(mAttrsMap.get(attr));
    }

    public int nextSymbol() {
      try {
        switch (mInput.nextToken()) {
          case StreamTokenizer.TT_EOL:
            mSymbol = EOL;
            break;
          case StreamTokenizer.TT_EOF:
            mSymbol = EOF;
            break;
          case StreamTokenizer.TT_WORD:
            if (matchAttributes(mInput.sval)) {
              mSymbol = TRUE;
            } else {
              mSymbol = FALSE;
            }
            break;
          case '(':
            mSymbol = LEFT;
            break;
          case ')':
            mSymbol = RIGHT;
            break;
          case '&':
            mSymbol = AND;
            break;
          case '|':
            mSymbol = OR;
            break;
          case '!':
            mSymbol = NOT;
            break;
          default:
            mSymbol = INVALID;
        }
      } catch (IOException e) {
        mSymbol = EOF;
      }
      return mSymbol;
    }
  }

  /**
   * Constraints Parser
   */
  private class Parser {
    private Lexer mLexer;
    private int mSymbol;
    private AttributeExpression mRoot;

    private final True mT = new True();
    private final False mF = new False();

    public Parser(Lexer lexer) {
      mLexer = lexer;
    }

    public AttributeExpression build() {
      expression();
      return mRoot;
    }

    private void expression() {
      term();
      while (mSymbol == Lexer.OR) {
        Or or = new Or();
        or.setLeft(mRoot);
        term();
        or.setRight(mRoot);
        mRoot = or;
      }
    }

    private void term() {
      factor();
      while (mSymbol == Lexer.AND) {
        And and = new And();
        and.setLeft(mRoot);
        factor();
        and.setRight(mRoot);
        mRoot = and;
      }
    }

    private void factor() {
      mSymbol = mLexer.nextSymbol();
      if (mSymbol == Lexer.TRUE) {
        mRoot = mT;
        mSymbol = mLexer.nextSymbol();
      } else if (mSymbol == Lexer.FALSE) {
        mRoot = mF;
        mSymbol = mLexer.nextSymbol();
      } else if (mSymbol == Lexer.NOT) {
        Not not = new Not();
        factor();
        not.setChild(mRoot);
        mRoot = not;
      } else if (mSymbol == Lexer.LEFT) {
        expression();
        mSymbol = mLexer.nextSymbol();
      } else {
        throw new RuntimeException("Expression Malformed");
      }
    }
  }

  private interface AttributeExpression {
    boolean interpret();
  }

  private abstract class NonTerminal implements AttributeExpression {
    protected AttributeExpression mLeft;
    protected AttributeExpression mRight;

    public void setLeft(AttributeExpression left) {
      mLeft = left;
    }

    public void setRight(AttributeExpression right) {
      mRight = right;
    }
  }

  private abstract class Terminal implements AttributeExpression {
    protected boolean mValue;

    public Terminal(boolean value) {
      mValue = value;
    }

    @Override
    public String toString() {
      return String.format("%s", mValue);
    }
  }

  private class And extends NonTerminal {

    @Override
    public boolean interpret() {
      return mLeft.interpret() && mRight.interpret();
    }

    @Override
    public String toString() {
      return String.format("(%s & %s)", mLeft, mRight);
    }
  }

  private class Or extends NonTerminal {

    @Override
    public boolean interpret() {
      return mLeft.interpret() || mRight.interpret();
    }

    @Override
    public String toString() {
      return String.format("(%s | %s)", mLeft, mRight);
    }
  }

  private class Not extends NonTerminal {
    public void setChild(AttributeExpression child) {
      setLeft(child);
    }

    @Override
    public void setRight(AttributeExpression right) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean interpret() {
      return !mLeft.interpret();
    }

    @Override
    public String toString() {
      return String.format("!%s", mLeft);
    }
  }

  private class False extends Terminal {
    public False() {
      super(false);
    }

    @Override
    public boolean interpret() {
      return mValue;
    }
  }

  private class True extends Terminal {
    public True() {
      super(true);
    }

    @Override
    public boolean interpret() {
      return mValue;
    }
  }
}
