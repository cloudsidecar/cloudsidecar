// Generated from Dynamo.g4 by ANTLR 4.7.

package parser // Dynamo

import "github.com/antlr/antlr4/runtime/Go/antlr"

// DynamoListener is a complete listener for a parse tree produced by DynamoParser.
type DynamoListener interface {
	antlr.ParseTreeListener

	// EnterStart is called when entering the start production.
	EnterStart(c *StartContext)

	// EnterExpression is called when entering the expression production.
	EnterExpression(c *ExpressionContext)

	// EnterOperand is called when entering the operand production.
	EnterOperand(c *OperandContext)

	// EnterComparator is called when entering the comparator production.
	EnterComparator(c *ComparatorContext)

	// ExitStart is called when exiting the start production.
	ExitStart(c *StartContext)

	// ExitExpression is called when exiting the expression production.
	ExitExpression(c *ExpressionContext)

	// ExitOperand is called when exiting the operand production.
	ExitOperand(c *OperandContext)

	// ExitComparator is called when exiting the comparator production.
	ExitComparator(c *ComparatorContext)
}
