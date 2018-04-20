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
    return SEMI;

}

{COMMA} {
    printf("get comma : %s\n", yytext);
    return COMMA;

}
{ASSIGNOP} {
    printf("get assignop : %s\n", yytext);
    return ASSIGNOP;

}

{INT} {
    printf("get an integer: %s\n", yytext);
    return INT;

}
_
{FLOAT} {
    printf("get a float: %s\n", yytext);
    return FLOAT;
}

{PLUS} {
    return PLUS;
}
{MINUS} {
    return MINUS;
}
{DIV} {
    return DIV;
}
{STAR} {
    printf("get an operator: %s\n", yytext);
    return STAR;
}

{RELOP} {
    printf("get a relop: %s\n", yytext);
    return RELOP;
}

{AND} {
    return AND;
}
{OR} {
    return OR;
}
{NOT} {
    printf("get a logic operator: %s\n", yytext);
    return NOT;
}

{LP} {
    return LP;
}
{RP} {
    return RP;
}
{LB} {
    return LB;
}
{RB} {
    return RB;
}
{LC} {
    return LC;
}
{RC} {
    printf("get brackets : %s\n", yytext);
    return RC;
}

{DOT} {
    printf("get a dot: %s\n", yytext);
    return DOT;
}

{TYPE} {
    printf("get type: %s\n", yytext);
    return TYPE;
}


{SPACE} {
/*ABANDON THESE CHARACTORS*/
    
}

{SNOTE} |
{DNOTE} {
	printf("this is a note: %s",yytext);
}
{STRUCT} {
    return STRUCT;
}
{RETURN} {
    return RETURN;
}
{IF} {
    return IF;
}
{ELSE} {
    return ELSE;
}
{WHILE} {
    printf("get keyword: %s\n", yytext);
    return WHILE;
}

{ID} {
    printf("get an ID: %s\n", yytext);
    return WHILE;
}
. {
	printf("error type A at line %d about  %s\n",yylineno,yytext);
}
%%
int yywrap() {
  return 1;
}
/*
int main(int argc, char** argv) {
   if (argc > 1) {
       if (!(yyin = fopen(argv[1], "r"))) {   
           perror(argv[1]);
           return 1;
       }
   }
   while (yylex());
   return 0;
}*/
