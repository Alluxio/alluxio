%{
#include "stdio.h"
#include "stdlib.h"

int compare = 0;
%}


FLOAT [0-9]*[.][0-9]+([eE][+-]?[0-9]*|[0])?f?
INT [1-9][0-9]*|[0]
SEMI [;]
COMMA [,]
ASSIGNOP [=]
RELOP [>]|[<]|[>][=]|[<][=]|[=][=]|[!][=](^[=])
PLUS [+]
MINUS [-]
STAR [*]
DIV [/]
AND [&][&]
OR [|][|]
DOT [.]
NOT [!]
TYPE int|float
LP \(
RP \)
LB \[
RB \]
LC \{
RC \}
STRUCT struct
RETURN return
IF if
ELSE else 
WHILE while
SPACE [ \n\t]
ID [a-zA-Z_][a-zA-Z_0-9]*


%%
{SEMI} {
    printf("get semmi : %s\n", yytext);
    compare = 1;
}

{COMMA} {
    printf("get comma : %s\n", yytext);
compare = 1;
}
{ASSIGNOP} {
    printf("get assignop : %s\n", yytext);
compare = 1;
}

{INT} {
    printf("get an integer: %s\n", yytext);
compare = 1;

}

{FLOAT} {
    printf("get a float: %s\n", yytext);
compare = 1;

}

{PLUS} | 
{MINUS} |
{DIV} |
{STAR} {
    printf("get an operator: %s\n", yytext);
compare = 1;

}

{RELOP} {
    printf("get a relop: %s\n", yytext);
compare = 1;

}

{AND} |
{OR} |
{NOT} {
    printf("get a logic operator: %s\n", yytext);
compare = 1;

}

{DOT} {
    printf("get a dot: %s\n", yytext);
compare = 1;

}
{STRUCT} |
{RETURN} |
{IF} |
{ELSE} |
{WHILE} {
    printf("get keyword: %s\n", yytext);
compare = 1;

}

{TYPE} {
    printf("get type: %s\n", yytext);
compare = 1;

}

{LP} |
{RP} |
{LB} |
{RB} |
{LC} |
{RC} {
    printf("get brackets : %s\n", yytext);
compare = 1;
}

{SPACE} |
. {
/*ABANDON THESE CHARACTORS*/
compare = 1;
}

{ID} {
    printf("get an ID: %s\n", yytext);
compare = 1;
}
. {
	printf("error:%s\n",yytext);
}

%%
int yywrap() {
  return 1;
}

int main(int argc, char** argv) {
   if (argc > 1) {
       if (!(yyin = fopen(argv[1], "r"))) {   
           perror(argv[1]);
           return 1;
       }
   }
   while (yylex())
   {	
	if(compare == 0)
	printf("error type a at line about %s\n",yytext);
	compare = 0;
   }
   
	

   return 0;
}
