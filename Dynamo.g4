grammar Dynamo;

// Tokens
EQ: '=';
NOT_EQ: '<>';
GT: '>';
GTE: '>=';
LT: '<';
LTE: '<=';
LPAREN: '(';
RPAREN: ')';
IN: 'IN' | 'in';
AND: 'AND' | 'and';
BETWEEN: 'BETWEEN' | 'between';
OR: 'OR' | 'or';
NOT: 'NOT' | 'not';
QUOTE: '"';
COMMA: ',';
// IDENT : [a-zA-Z][a-zA-Z0-9_.]*;
VALUE_HOLDER: [:][a-zA-Z]+[0-9]+;
IDENT_HOLDER: [#][a-zA-Z]+[0-9]+;
WHITESPACE: [ \r\n\t]+ -> skip;
// STRING : '"' (options{greedy=false;}:( ~('\\'|'"') | ('\\' '"')))* '"';
// NUMBER: [0-9]+;

// Rules
start : expression EOF;

expression
   : IDENT_HOLDER comparator VALUE_HOLDER
   | IDENT_HOLDER BETWEEN VALUE_HOLDER AND VALUE_HOLDER
   | IDENT_HOLDER IN LPAREN VALUE_HOLDER (COMMA VALUE_HOLDER)* RPAREN
   | expression AND expression
   | expression OR expression
   | NOT expression
   | LPAREN expression RPAREN
   ;

operand
   : VALUE_HOLDER
   | IDENT_HOLDER
   ;


comparator
   : EQ
   | NOT_EQ
   | GT
   | GTE
   | LTE
   ;

