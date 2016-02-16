package tachyon.mesos;

import org.apache.mesos.Protos;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AttributeEvaluator {

  private Map<String, String> attrsMap = new HashMap<String, String>();

  private void setAttributes(List<Protos.Attribute> attributes) {
    if (attributes == null || attributes.isEmpty()) {
      return;
    }

    for (Protos.Attribute attr : attributes) {
      // Mesos attribute : text ":" ( scalar | range | text )
      // now only support text attributes.
      attrsMap.put(attr.getName(), attr.getText().getValue());
    }
  }

  public AttributeEvaluator(List<Protos.Attribute> attributes) {
    setAttributes(attributes);
  }

  public boolean matchAttributes(String constraints) {
    Lexer lexer = new Lexer(constraints);
    Parser parser = new Parser(lexer);
    AttributeExpression expression = parser.build();
    return expression.interpret();
  }

  private class Lexer {
    private StreamTokenizer input;

    private int symbol = NONE;
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

    public Lexer(String constraints) {
      Reader r = buildReader(constraints);
      input = new StreamTokenizer(r);
      resetSyntax();
    }

    private void resetSyntax() {
      input.resetSyntax();
      input.wordChars('0', 'z');
      input.whitespaceChars('\u0000', ' ');
      input.whitespaceChars('\n', '\t');

      input.ordinaryChar('(');
      input.ordinaryChar(')');
      input.ordinaryChar('&');
      input.ordinaryChar('|');
      input.ordinaryChar('!');
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

      return text.equals(attrsMap.get(attr));
    }

    public int nextSymbol() {
      try {
        switch (input.nextToken()) {
          case StreamTokenizer.TT_EOL:
            symbol = EOL;
            break;
          case StreamTokenizer.TT_EOF:
            symbol = EOF;
            break;
          case StreamTokenizer.TT_WORD:
            if (matchAttributes(input.sval)) {
              symbol = TRUE;
            } else {
              symbol = FALSE;
            }
            break;
          case '(':
            symbol = LEFT;
            break;
          case ')':
            symbol = RIGHT;
            break;
          case '&':
            symbol = AND;
            break;
          case '|':
            symbol = OR;
            break;
          case '!':
            symbol = NOT;
            break;
          default:
            symbol = INVALID;
        }
      } catch (IOException e) {
        symbol = EOF;
      }
      return symbol;
    }
  }

  private class Parser {
    private Lexer lexer;
    private int symbol;
    private AttributeExpression root;

    private final True t = new True();
    private final False f = new False();

    public Parser(Lexer lexer) {
      this.lexer = lexer;
    }

    public AttributeExpression build() {
      expression();
      return root;
    }

    private void expression() {
      term();
      while (symbol == Lexer.OR) {
        Or or = new Or();
        or.setLeft(root);
        term();
        or.setRight(root);
        root = or;
      }
    }

    private void term() {
      factor();
      while (symbol == Lexer.AND) {
        And and = new And();
        and.setLeft(root);
        factor();
        and.setRight(root);
        root = and;
      }
    }

    private void factor() {
      symbol = lexer.nextSymbol();
      if (symbol == Lexer.TRUE) {
        root = t;
        symbol = lexer.nextSymbol();
      } else if (symbol == Lexer.FALSE) {
        root = f;
        symbol = lexer.nextSymbol();
      } else if (symbol == Lexer.NOT) {
        Not not = new Not();
        factor();
        not.setChild(root);
        root = not;
      } else if (symbol == Lexer.LEFT) {
        expression();
        symbol = lexer.nextSymbol();
      } else {
        throw new RuntimeException("Expression Malformed");
      }
    }
  }

  private interface AttributeExpression {
    boolean interpret();
  }

  private abstract class NonTerminal implements AttributeExpression {
    protected AttributeExpression left, right;

    public void setLeft(AttributeExpression left) {
      this.left = left;
    }

    public void setRight(AttributeExpression right) {
      this.right = right;
    }
  }

  private abstract class Terminal implements AttributeExpression {
    protected boolean value;

    public Terminal(boolean value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return String.format("%s", value);
    }
  }

  private class And extends NonTerminal {

    @Override
    public boolean interpret() {
      return left.interpret() && right.interpret();
    }

    @Override
    public String toString() {
      return String.format("(%s & %s)", left, right);
    }
  }

  private class Or extends NonTerminal {

    @Override
    public boolean interpret() {
      return left.interpret() || right.interpret();
    }

    @Override
    public String toString() {
      return String.format("(%s | %s)", left, right);
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
      return !left.interpret();
    }

    @Override
    public String toString() {
      return String.format("!%s", left);
    }
  }

  private class False extends Terminal {
    public False() {
      super(false);
    }

    public boolean interpret() {
      return value;
    }
  }

  private class True extends Terminal {
    public True() {
      super(true);
    }

    public boolean interpret() {
      return value;
    }
  }
}