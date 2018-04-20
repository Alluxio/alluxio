%{
#include "stdio.h"
#include "stdlib.h"
%}

SPACE [ \n\r\t]+
SNOTE [/][/]([0-9A-Za-z]|{SPACE})*[\n]
DNOTE [/][*]([0-9]|[A-Za-z]|{SPACE})*[*][/]
INT [1-9][0-9]*|[0]
FLOAT [0-9]*[.][0-9]+([eE][+-]?[0-9]*|[0])?f?
SEMI [;]
COMMA [,]
ASSIGNOP [=]
PLUS [+]
MINUS [-]
STAR [*]
RELOP [>]|[<]|[>][=]|[<][=]|[=][=]|[!][=](^[=])
AND [&][&]
OR [|][|]
DOT [.]
NOT [!]
DIV [/]
LP \(
RP \)
LB \[
RB \]
LC \{
RC \}
TYPE int|float
STRUCT struct
RETURN return
IF if
ELSE else 
WHILE while
ID [a-zA-Z_][a-zA-Z_0-9]*

/*end of definition*/

%%
{SEMI} {
    printf("get semmi : %s\n", yytext);

}

{COMMA} {
    printf("get comma : %s\n", yytext);
}
{ASSIGNOP} {
    printf("get assignop : %s\n", yytext);
}

{INT} {
    printf("get an integer: %s\n", yytext);
}
_
{FLOAT} {
    printf("get a float: %s\n", yytext);
}

{PLUS} | 
{MINUS} |
{DIV} |
{STAR} {
    printf("get an operator: %s\n", yytext);
}

{RELOP} {
    printf("get a relop: %s\n", yytext);
}

{AND} |
{OR} |
{NOT} {
    printf("get a logic operator: %s\n", yytext);
}

{LP} |
{RP} |
{LB} |
{RB} |
{LC} |
{RC} {
    printf("get brackets : %s\n", yytext);
}

{DOT} {
    printf("get a dot: %s\n", yytext);
}

{TYPE} {
    printf("get type: %s\n", yytext);
}


{SPACE} {
/*ABANDON THESE CHARACTORS*/

}

{SNOTE} |
{DNOTE} {
	printf("this is a note: %s",yytext);
}
{STRUCT} |
{RETURN} |
{IF} |
{ELSE} |
{WHILE} {
    printf("get keyword: %s\n", yytext);
}

{ID} {
    printf("get an ID: %s\n", yytext);
}
. {
	printf("error type A at line %d about  %s\n",yylineno,yytext);
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
   while (yylex());
   return 0;
}
