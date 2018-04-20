#include "tree.h"
void add(Node* father,int loc,Node* child);
static int depth=-1;
static const char *const types_name_table[] =
{

	"SEMI","COMMA","ASSIGNOP","RELOP",
	"PLUS","MINUS","STAR","DIV",
	"AND","OR","DOT","NOT","TYPE",
	"LP","RP","LB","RB","LC","RC",
	"STRUCT","RETURN","IF","ELSE","WHILE",
	"ID","INT","FLOAT"
};


