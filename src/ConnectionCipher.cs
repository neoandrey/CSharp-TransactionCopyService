using System.Configuration;
using System.Collections.Specialized;
using System;
using System.Text;

namespace ArbiterCopyService{
	
public class ConnectionCipher {
    
    private static  string zero ="+~!~~!~$$$$$";
    private static  string one  ="~~!~~!~$$$$";
    private static  string two  = "#=~!~$$$$$";
    private static  string three= "*~!~~!~$$$$$";
    private static  string four = "&~!~~!~$$$$$";
    private static  string five = "~!~~!~^$$$$$";
    private static  string six  = "~!~~!~#%$$$$";
    private static  string seven="~!~~!~#$$$$$";
    private static  string eight="~!~~!~#$$#$$";
    private static  string nine="~!~~!~#$$$!$";
    private static  string a="~!@@£$@#$%^&*()_+~!@@£$@#$%^&*()_+"; 
    private static  string b="!@@£$@#$%^&*()_+~!@@£$@#$%^&*()_+~";
    private static  string c="@@£$@#$%^&*()_+~!@@£$@#$%^&*()_+~!";
    private static  string d="#$%^&*()_+~!@@£$@#$%^&*()_+~!@@£$@";
    private static  string e="$%^&*()_+~!@@£$@#$%^&*()_+~!@@£$@#";
    private static  string f="%^&*()_+~!@@£$@#$%^&*()_+~!@@£$@#$";
    private static  string g="^&*()_+~!@@£$@#$%^&*()_+~!@@£$@#$%";
    private static  string h="&*()_+~!@@£$@#$%^&*()_+~!@@£$@#$%^";
    private static  string i="*()_+~!@@£$@#$%^&*()_+~!@@£$@#$%^&";
    private static  string j="()_+~!@@£$@#$%^&*()_+~!@@£$@#$%^&*";
    private static  string k= ")_+~!@@£$@#$%^&*()_+~!@@£$@#$%^&*(";
    private static  string l="_+~!@@£$@#$%^&*()_+~!@@£$@#$%^&*()";
    private static  string m="+~!@@£$@#$%^&*()_+~!@@£$@#$%^&*()_";
    private static  string n="+_)(*&^%$#@@£$@!~+_)(*&^%$#@@£$@!~";
    private static  string o= "_)(*&^%$#@@£$@!~+_)(*&^%$#@@£$@!~+";
    private static  string p=")(*&^%$#@@£$@!~+_)(*&^%$#@@£$@!~_"; 
    private static  string  q="(*&^%$#@@£$@!~+_)(*&^%$#@@£$@!~+_)";
    private static  string r="*&^%$#@@£$@!~+_)(*&^%$#@@£$@!~+_)(";
    private static  string s="&^%$#@@£$@!~+_)(*&^%$#@@£$@!~+_)(*";
    private static  string t="^%$#@@£$@!~+_)(*&^%$#@@£$@!~+_)(*&";
    private static  string u="%$#@@£$@!~+_)(*&^%$#@@£$@!~+_)(*&^";
    private static  string v="$#@@£$@!~+_)(*&^%$#@@£$@!~+_)(*&^%";
    private static  string w="#@@£$@!~+_)(*&^%$#@@£$@!~+_)(*&^%$";
    private static  string x="@@£$@!~+_)(*&^%$#@@£$@!~+_)(*&^%$#";
    private static  string y="!~+_)(*&^%$#@@£$@!~+_)(*&^%$#@@£$@";
    private static  string z="~+_)(*&^%$#@@£$@!~+_)(*&^%$#@@£$@!";
    
    private static  string A="~!@@£$@#$%^&*()_+.~!@@£$@#$%^&*()_+"; 
    private static  string B="!@@£$@#$%^&*()_+~.!@@£$@#$%^&*()_+~";
    private static  string C="@@£$@#$%^&*()_+~.!@@£$@#$%^&*()_+~!";
    private static  string D="#$%^&*()_+~!@@£$@.#$%^&*()_+~!@@£$@";
    private static  string E="$%^&*()_+~!@@£$@#.$%^&*()_+~!@@£$@#";
    private static  string F="%^&*()_+~!@@£$@#.$%^&*()_+~!@@£$@#$";
    private static  string G="^&*()_+~!@@£$@#$.%^&*()_+~!@@£$@#$%";
    private static  string H="&*()_+~!@@£$@#$%^.&*()_+~!@@£$@#$%^";
    private static  string I="*()_+~!@@£$@#$%^&.*()_+~!@@£$@#$%^&";
    private static  string J="()_+~!@@£$@#$%^&.*()_+~!@@£$@#$%^&*";
    private static  string K= ")_+~!@@£$@#$%^&*(.)_+~!@@£$@#$%^&*(";
    private static  string L="_+~!@@£$@#$%^&*()._+~!@@£$@#$%^&*()";
    private static  string M="+~!@@£$@#$%^&*()_.+~!@@£$@#$%^&*()_";
    private static  string N="+_)(*&^%$#@@£$@!.~+_)(*&^%$#@@£$@!~";
    private static  string O= "_)(*&^%$#@@£$@!~.+_)(*&^%$#@@£$@!~+";
    private static  string P=")(*&^%$#@@£$@!~+._)(*&^%$#@@£$@!~_"; 
    private static  string Q="(*&^%$#@@£$@!~+_).(*&^%$#@@£$@!~+_)";
    private static  string R="*&^%$#@@£$@!~+_)(.*&^%$#@@£$@!~+_)(";
    private static  string S="&^%$#@@£$@!~+_)(.*&^%$#@@£$@!~+_)(*";
    private static  string T="^%$#@@£$@!~+_)(*.&^%$#@@£$@!~+_)(*&";
    private static  string U="%$#@@£$@!~+_)(*&^.%$#@@£$@!~+_)(*&^";
    private static  string V="$#@@£$@!~+_)(*&^%.$#@@£$@!~+_)(*&^%";
    private static  string W="#@@£$@!~+_)(*&^%$.#@@£$@!~+_)(*&^%$";
    private static  string X="@@£$@!~+_)(*&^%$#.@@£$@!~+_)(*&^%$#";
    private static  string Y="!~+_)(*&^%$#@@£$@.!~+_)(*&^%$#@@£$@";
    private static  string Z="~+_)(*&^%$#@@£$@!.~+_)(*&^%$#@@£$@!";
    private static  string sep="|";
    private static  string exclamation ="[]{}:;?<>,";        
    private static  string asterix="]{}:;?<>,[";    
    private static  string pound="{}:;?<>,[]";
    private static  string dols= "}:;?<>,[]{";
    private static  string percentage=":;?<>,[]{}";
    private static  string ampersand=";?<>,[]{}:";
    private static  string singleQuote="?<>,[]{}:;";
    private static  string apostrophe="??<>{}[]:;,";
    private static  string leftParenthesis= "<>,[]{}:;?";
    private static  string rightParenthesis=">,[]{}:;?<";
    private static  string doubleQuote=",[]{}:;?<>";
    private static  string plus= "?>,[]{}:;<";
    private static  string comma="?<,[]{}:;>";
    private static  string hyphen="?[],{}:;<>";
    private static  string dot="?[,{}:;<>]";
    private static  string forSlash=   "?{[;<,>:]}";
    private static  string backSlash="?,:<>[]{};";
    private static  string colon= "<{[;,:]}?>";
    private static  string semicolon= "<{[:?;]}>,";
    private static  string lessThan = ">[;,?{}:]<";
    private static  string greaterThan=">[;},{?:]<";
    private static  string equality=  ":<[]{}>;,?";
    private static  string questionMark=">,}]?[{<";
    private static  string at=",>?{[}]<;:";
    private static  string leftBrace =":{}[,]<>?;";
    private static  string  rightBrace="},[;:}{<>?";
    private static  string index="}?,;<:}][";
    private static  string underscore="},:]{?>[<";
    private static  string leftBracket=";,>{<}[]?:";
    private static  string rightBracket=";]{:}[,<?>";
    private static  string tilde="[?{;:}]<>,";
    private static  string space ="[,<:>}{/]";
    private static  string pipe ="{;:<,?>[]}";
	
	public ConnectionCipher(){
		
		
	}
  
   static string charConverter(string raw){
      string converted=sep;
      if (raw.Equals("0")){
      converted= zero;
      }
    else  if (raw.Equals("1")){
      converted= one;
      }
     else if (raw.Equals("2")){
      converted= two;
      }
      else if (raw.Equals("3")){
      converted= three;
      }
    else  if (raw.Equals("4")){
      converted= four;
      }
    else  if (raw.Equals("5")){
      converted= five;
      }
     else if (raw.Equals("6")){
      converted= six;
      }
    else  if (raw.Equals("7")){
      converted= seven;
      }
    else  if (raw.Equals("8")){
      converted= eight;
      }
    else  if (raw.Equals("9")){
      converted= nine;
      }
    else  if (raw.Equals("a")){
      converted= a;
      }
     else  if (raw.Equals("b")){
      converted= b;
      }
     else  if (raw.Equals("c")){
      converted= c;
      }
    else   if (raw.Equals("d")){
      converted= d;
      }
      else if (raw.Equals("e")){
      converted= e;
      }
      else if (raw.Equals("f")){
      converted= f;
      }
      else if (raw.Equals("g")){
      converted= g;
      }
    else if (raw.Equals("h")){
      converted= h;
      }
     else if (raw.Equals("i")){
      converted= i;
      }
      else if (raw.Equals("j")){
      converted= j;
      }
     else  if (raw.Equals("k")){
      converted= k;
      } 
      else if (raw.Equals("l")){
      converted= l;
      }
      else if (raw.Equals("m")){
      converted= m;
      }
      else if (raw.Equals("n")){
      converted= n;
      }
      else if (raw.Equals("o")){
      converted= o;
      }
      else if (raw.Equals("p")){
      converted= p;
      }
      else if (raw.Equals("q")){
      converted= q;
      }
      else if (raw.Equals("r")){
      converted= r;
      }
      else if (raw.Equals("s")){
      converted= s;
      }
      else if (raw.Equals("t")){
      converted= t;
      }
     else if (raw.Equals("u")){
      converted= u;
      }
     else if (raw.Equals("v")){
      converted= v;
      }
      else if (raw.Equals("w")){
      converted= w;
      }
     else  if (raw.Equals("x")){
      converted= x;
      }
     else  if (raw.Equals("y")){
      converted= y;
      }
     else if (raw.Equals("z")){
      converted= z;
      }
      else if (raw.Equals("A")){
      converted= A;
      }
     else   if (raw.Equals("B")){
      converted= B;
      }
     else  if (raw.Equals("C")){
      converted= C;
      }
     else  if (raw.Equals("D")){
      converted= D;
      }
     else  if (raw.Equals("E")){
      converted= E;
      }
      else if (raw.Equals("F")){
      converted= F;
      }
      else if (raw.Equals("G")){
      converted= G;
      }
      else if (raw.Equals("H")){
      converted= H;
      }
      else if (raw.Equals("I")){
      converted= I;
      }
      else if (raw.Equals("J")){
      converted= J;
      }
      else if (raw.Equals("K")){
      converted= K;
      }
      else if (raw.Equals("L")){
      converted= L;
      }
      else if (raw.Equals("M")){
      converted= M;
      }
      else if (raw.Equals("N")){
      converted= N;
      }
      else if (raw.Equals("O")){
      converted= O;
      }
      else if (raw.Equals("P")){
      converted= P;
      }
      else if (raw.Equals("Q")){
      converted= Q;
      }
      else if (raw.Equals("R")){
      converted= R;
      }
      else if (raw.Equals("S")){
      converted= S;
      }
      else if (raw.Equals("T")){
      converted= T;
      }
     else if (raw.Equals("U")){
      converted= U;
      }
      else if (raw.Equals("V")){
      converted= V;
      }
      else if (raw.Equals("W")){
      converted= W;
      }
      else if (raw.Equals("X")){
      converted= X;
      }
      else if (raw.Equals("Y")){
      converted= Y;
      }
      else if (raw.Equals("Z")){
      converted= Z;
      }
      else if (raw.Equals("!")){
      converted= exclamation;
      }
       else if (raw.Equals("`")){
      converted= apostrophe;
      }
     else  if (raw.Equals("*")){
      converted= asterix;
      }
      else if (raw.Equals("#")){
      converted= pound;
      }
      else if (raw.Equals("$")){
      converted=dols;
      }
      else if (raw.Equals("%")){
      converted= percentage;
      }
      else if (raw.Equals("&")){
      converted= ampersand;
      }
      else if (raw.Equals("\'")){
      converted= singleQuote;
      }
      else if (raw.Equals("(")){
      converted= leftParenthesis;
      }
      else if (raw.Equals(")")){
      converted= rightParenthesis;
      }
      else if (raw.Equals("\"")){
      converted= doubleQuote;
      }
      else if (raw.Equals("+")){
      converted=  plus;
      }
      else if (raw.Equals(",")){
      converted= comma;
      } 
      else if (raw.Equals("-")){
      converted= hyphen;
      }
      else if (raw.Equals(".")){
      converted= dot;
      }
       else if (raw.Equals("/")){
      converted= forSlash;
      }
      else if (raw.Equals("\\")){
      converted= backSlash;
      }
      else if (raw.Equals(":")){
      converted= colon;
      }
      else if (raw.Equals(";")){
      converted=semicolon;
      }
      else if (raw.Equals("<")){
      converted= lessThan;
      }
      else if (raw.Equals(">")){
      converted= greaterThan;
      }
      else if (raw.Equals("=")){
      converted= equality;
      }
      else if (raw.Equals("?")){
      converted= questionMark;
      }
      else if (raw.Equals("@")){
      converted= at;
      }
      else if (raw.Equals("[")){
      converted= leftBrace;
      }
      else if (raw.Equals("]")){
      converted= rightBrace;
      }
     else if(raw.Equals("^")){
      converted=index;
      }
      else if(raw.Equals("_")){
      converted=underscore;
      }
      else if(raw.Equals("{")){
      converted=leftBracket;
      }
     else if(raw.Equals("}")){
      converted=rightBracket;
      }
      else if(raw.Equals("~")){
      converted=tilde;
      }
      else if(raw.Equals(" ")){
      converted=space;
      }
      else if(raw.Equals("|")){
      converted=pipe;
      }
      
      return converted;
  
  }
   static string retriever(string coded){
      string converted=sep;
      if (zero.Equals(coded)){
      converted= "0";
      }
      else if (one.Equals(coded)){
      converted= "1";
      }
      else if (two.Equals(coded)){
      converted= "2";
      }
      else if (three.Equals(coded)){
      converted= "3";
      }
      else if (four.Equals(coded)){
      converted= "4";
      }
      else if (five.Equals(coded)){
      converted= "5";
      }
      else if (six.Equals(coded)){
      converted= "6";
      }
      else if (seven.Equals(coded)){
      converted= "7";
      }
      else if (eight.Equals(coded)){
      converted= "8";
      }
      else if (nine.Equals(coded)){
      converted= "9";
      }
      else if (a.Equals(coded)){
      converted= "a";
      }
      else if (b.Equals(coded)){
      converted= "b";
      }
      else if (c.Equals(coded)){
      converted= "c";
      }
      else if (d.Equals(coded)){
      converted= "d";
      }
      else if (e.Equals(coded)){
      converted= "e";
      }
      else if (f.Equals(coded)){
      converted= "f";
      }
      else if (g.Equals(coded)){
      converted= "g";
      }
      else if (h.Equals(coded)){
      converted= "h";
      }
      else if (i.Equals(coded)){
      converted= "i";
      }
      else if (j.Equals(coded)){
      converted= "j";
      }
      else if (k.Equals(coded)){
      converted= "k";
      }
      else if (l.Equals(coded)){
      converted= "l";
      }
      else if (m.Equals(coded)){
      converted= "m";
      }
      else if (n.Equals(coded)){
      converted= "n";
      }
      else if (o.Equals(coded)){
      converted= "o";
      }
      else if (p.Equals(coded)){
      converted= "p";
      }
      else if (q.Equals(coded)){
      converted= "q";
      }
      else if (r.Equals(coded)){
      converted= "r";
      }
      else if (s.Equals(coded)){
      converted= "s";
      }
      else if (t.Equals(coded)){
      converted= "t";
      }
      else if (u.Equals(coded)){
      converted= "u";
      }
      else if (v.Equals(coded)){
      converted= "v";
      }
      else if (w.Equals(coded)){
      converted= "w";
      }
      else if (x.Equals(coded)){
      converted= "x";
      }
      else if (y.Equals(coded)){
      converted= "y";
      }
      else if (z.Equals(coded)){
      converted= "z";
      }
       else if (A.Equals(coded)){
      converted= "A";
      }
      else if (B.Equals(coded)){
      converted= "B";
      }
      else if (C.Equals(coded)){
      converted= "C";
      }
      else if (D.Equals(coded)){
      converted= "D";
      }
      else if (E.Equals(coded)){
      converted= "E";
      }
      else if (F.Equals(coded)){
      converted= "F";
      }
      else if (G.Equals(coded)){
      converted= "G";
      }
      else if (H.Equals(coded)){
      converted= "H";
      }
      else if (I.Equals(coded)){
      converted= "I";
      }
      else if (J.Equals(coded)){
      converted= "J";
      }
      else if (K.Equals(coded)){
      converted= "K";
      }
      else if (L.Equals(coded)){
      converted= "L";
      } 
      else if (M.Equals(coded)){
      converted= "M";
      }
      else if (N.Equals(coded)){
      converted= "N";
      }
      else if (O.Equals(coded)){
      converted= "O";
      }
      else if (P.Equals(coded)){
      converted= "P";
      }
      else if (Q.Equals(coded)){
      converted= "Q";
      }
      else if (R.Equals(coded)){
      converted= "R";
      }
     else  if (S.Equals(coded)){
      converted= "S";
      }
     else if (T.Equals(coded)){
      converted= "T";
      }
    else  if (U.Equals(coded)){
      converted= "U";
      }
    else  if (V.Equals(coded)){
      converted= "V";
      }
     else  if (W.Equals(coded)){
      converted= "W";
      }
     else  if (X.Equals(coded)){
      converted= "X";
      }
     else  if (Y.Equals(coded)){
      converted= "Y";
      }
      else if (Z.Equals(coded)){
      converted= "Z";
      }
     else if (exclamation.Equals(coded)){
      converted= "!";
      }
     else  if (asterix.Equals(coded)){
      converted= "*";
      }
      else  if (apostrophe.Equals(coded)){
      converted= "`";
      }
     else  if (pound.Equals(coded)){
      converted= "#";
      }
      else if (dols.Equals(coded)){
      converted= "$";
      }
      else if (percentage.Equals(coded)){
      converted= "%";
      }
      else if (ampersand.Equals(coded)){
      converted= "&";
      }
     else  if (singleQuote.Equals(coded)){
      converted= "'";
      }
     else  if (leftParenthesis.Equals(coded)){
      converted="(" ;
      }
     else  if (rightParenthesis.Equals(coded)){
      converted= ")";
      }
      else if (doubleQuote.Equals(coded)){
      converted="\"" ;
      }
      else if (plus.Equals(coded)){
      converted="+"  ;
      }
      else if (comma.Equals(coded)){
      converted= ",";
      }
      else if (hyphen.Equals(coded)){
      converted= "-";
      }
      else if (dot.Equals(coded)){
      converted= ".";
      }
     else  if (forSlash.Equals(coded)){
      converted= "/";
      }
      else if (backSlash.Equals(coded)){
      converted= "\\";
      }
      else if (colon.Equals(coded)){
      converted= ":";
      }
      else if (semicolon.Equals(coded)){
      converted=";";
      }
     else if (lessThan.Equals(coded)){
      converted= "<";
      }
      else if (greaterThan.Equals(coded)){
      converted= ">";
      }
     else if (equality.Equals(coded)){
      converted= "=";
      }
     else  if (questionMark.Equals(coded)){
      converted= "?";
      }
      else if (at.Equals(coded)){
      converted= "@";
      }
     else  if (leftBrace.Equals(coded)){
      converted= "[";
      }
     else  if (rightBrace.Equals(coded)){
      converted= "]";
      }
      else if(index.Equals(coded)){
      converted="^";
      }
      else if(underscore.Equals(coded)){
      converted="_";
      }
     else if(leftBracket.Equals(coded)){
      converted="{";
      }
     else if(rightBracket.Equals(coded)){
      converted="}";
      }
      else if(tilde.Equals(coded)){
      converted="~";
      }
     else  if(space.Equals(coded)){
      converted=" ";
      }
     else  if(pipe.Equals(coded)){
      converted="|";
      }
      
      return converted;
  
  }
  public  string scramble(string plain){
      string coded="";
	  try{
      int span = plain.Length;
      for (int cursor=0; cursor< span; cursor++){
      string temp=""+plain[cursor];
      coded+=charConverter(temp);
      coded+=sep;
      }
	  	  
	  }catch(Exception e){
		  Console.WriteLine(e.Message);
		  Console.WriteLine(e.StackTrace);
	  }
	  
      return coded;
  }
  public  string decipher(string coded){
      string plain="";
      int span = coded.Length;
      string buf="";
      for(int cursor=0; cursor< span; cursor++){
       string temp=""+coded[cursor];
       if (!temp.Equals("|"))buf+=coded[cursor];
       else{
       plain+=retriever(buf);
       buf="";
       }
  }
  
  return plain;
  }
    
}
}