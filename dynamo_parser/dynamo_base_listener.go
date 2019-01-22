// Generated from Dynamo.g4 by ANTLR 4.7.

package parser // Dynamo

import "github.com/antlr/antlr4/runtime/Go/antlr"

// BaseDynamoListener is a complete listener for a parse tree produced by DynamoParser.
type BaseDynamoListener struct{}

var _ DynamoListener = &BaseDynamoListener{}

// VisitTerminal is called when a terminal node is visited.
func (s *BaseDynamoListener) VisitTerminal(node antlr.TerminalNode) {}

// VisitErrorNode is called when an error node is visited.
func (s *BaseDynamoListener) VisitErrorNode(node antlr.ErrorNode) {}

// EnterEveryRule is called when any rule is entered.
func (s *BaseDynamoListener) EnterEveryRule(ctx antlr.ParserRuleContext) {}

// ExitEveryRule is called when any rule is exited.
func (s *BaseDynamoListener) ExitEveryRule(ctx antlr.ParserRuleContext) {}

// EnterStart is called when production start is entered.
func (s *BaseDynamoListener) EnterStart(ctx *StartContext) {}

// ExitStart is called when production start is exited.
func (s *BaseDynamoListener) ExitStart(ctx *StartContext) {}

// EnterExpression is called when production expression is entered.
func (s *BaseDynamoListener) EnterExpression(ctx *ExpressionContext) {}

// ExitExpression is called when production expression is exited.
func (s *BaseDynamoListener) ExitExpression(ctx *ExpressionContext) {}

// EnterOperand is called when production operand is entered.
func (s *BaseDynamoListener) EnterOperand(ctx *OperandContext) {}

// ExitOperand is called when production operand is exited.
func (s *BaseDynamoListener) ExitOperand(ctx *OperandContext) {}

// EnterComparator is called when production comparator is entered.
func (s *BaseDynamoListener) EnterComparator(ctx *ComparatorContext) {}

// ExitComparator is called when production comparator is exited.
func (s *BaseDynamoListener) ExitComparator(ctx *ComparatorContext) {}
