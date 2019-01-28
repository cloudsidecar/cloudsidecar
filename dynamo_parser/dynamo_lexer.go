// Generated from Dynamo.g4 by ANTLR 4.7.

package parser

import (
	"fmt"
	"unicode"

	"github.com/antlr/antlr4/runtime/Go/antlr"
)

// Suppress unused import error
var _ = fmt.Printf
var _ = unicode.IsLetter

var serializedLexerAtn = []uint16{
	3, 24715, 42794, 33075, 47597, 16764, 15335, 30598, 22884, 2, 20, 137,
	8, 1, 4, 2, 9, 2, 4, 3, 9, 3, 4, 4, 9, 4, 4, 5, 9, 5, 4, 6, 9, 6, 4, 7,
	9, 7, 4, 8, 9, 8, 4, 9, 9, 9, 4, 10, 9, 10, 4, 11, 9, 11, 4, 12, 9, 12,
	4, 13, 9, 13, 4, 14, 9, 14, 4, 15, 9, 15, 4, 16, 9, 16, 4, 17, 9, 17, 4,
	18, 9, 18, 4, 19, 9, 19, 3, 2, 3, 2, 3, 3, 3, 3, 3, 3, 3, 4, 3, 4, 3, 5,
	3, 5, 3, 5, 3, 6, 3, 6, 3, 7, 3, 7, 3, 7, 3, 8, 3, 8, 3, 9, 3, 9, 3, 10,
	3, 10, 3, 10, 3, 10, 5, 10, 63, 10, 10, 3, 11, 3, 11, 3, 11, 3, 11, 3,
	11, 3, 11, 5, 11, 71, 10, 11, 3, 12, 3, 12, 3, 12, 3, 12, 3, 12, 3, 12,
	3, 12, 3, 12, 3, 12, 3, 12, 3, 12, 3, 12, 3, 12, 3, 12, 5, 12, 87, 10,
	12, 3, 13, 3, 13, 3, 13, 3, 13, 5, 13, 93, 10, 13, 3, 14, 3, 14, 3, 14,
	3, 14, 3, 14, 3, 14, 5, 14, 101, 10, 14, 3, 15, 3, 15, 3, 16, 3, 16, 3,
	17, 3, 17, 7, 17, 109, 10, 17, 12, 17, 14, 17, 112, 11, 17, 3, 17, 6, 17,
	115, 10, 17, 13, 17, 14, 17, 116, 3, 18, 3, 18, 7, 18, 121, 10, 18, 12,
	18, 14, 18, 124, 11, 18, 3, 18, 6, 18, 127, 10, 18, 13, 18, 14, 18, 128,
	3, 19, 6, 19, 132, 10, 19, 13, 19, 14, 19, 133, 3, 19, 3, 19, 2, 2, 20,
	3, 3, 5, 4, 7, 5, 9, 6, 11, 7, 13, 8, 15, 9, 17, 10, 19, 11, 21, 12, 23,
	13, 25, 14, 27, 15, 29, 16, 31, 17, 33, 18, 35, 19, 37, 20, 3, 2, 7, 3,
	2, 60, 60, 4, 2, 67, 92, 99, 124, 3, 2, 50, 59, 3, 2, 37, 37, 5, 2, 11,
	12, 15, 15, 34, 34, 2, 146, 2, 3, 3, 2, 2, 2, 2, 5, 3, 2, 2, 2, 2, 7, 3,
	2, 2, 2, 2, 9, 3, 2, 2, 2, 2, 11, 3, 2, 2, 2, 2, 13, 3, 2, 2, 2, 2, 15,
	3, 2, 2, 2, 2, 17, 3, 2, 2, 2, 2, 19, 3, 2, 2, 2, 2, 21, 3, 2, 2, 2, 2,
	23, 3, 2, 2, 2, 2, 25, 3, 2, 2, 2, 2, 27, 3, 2, 2, 2, 2, 29, 3, 2, 2, 2,
	2, 31, 3, 2, 2, 2, 2, 33, 3, 2, 2, 2, 2, 35, 3, 2, 2, 2, 2, 37, 3, 2, 2,
	2, 3, 39, 3, 2, 2, 2, 5, 41, 3, 2, 2, 2, 7, 44, 3, 2, 2, 2, 9, 46, 3, 2,
	2, 2, 11, 49, 3, 2, 2, 2, 13, 51, 3, 2, 2, 2, 15, 54, 3, 2, 2, 2, 17, 56,
	3, 2, 2, 2, 19, 62, 3, 2, 2, 2, 21, 70, 3, 2, 2, 2, 23, 86, 3, 2, 2, 2,
	25, 92, 3, 2, 2, 2, 27, 100, 3, 2, 2, 2, 29, 102, 3, 2, 2, 2, 31, 104,
	3, 2, 2, 2, 33, 106, 3, 2, 2, 2, 35, 118, 3, 2, 2, 2, 37, 131, 3, 2, 2,
	2, 39, 40, 7, 63, 2, 2, 40, 4, 3, 2, 2, 2, 41, 42, 7, 62, 2, 2, 42, 43,
	7, 64, 2, 2, 43, 6, 3, 2, 2, 2, 44, 45, 7, 64, 2, 2, 45, 8, 3, 2, 2, 2,
	46, 47, 7, 64, 2, 2, 47, 48, 7, 63, 2, 2, 48, 10, 3, 2, 2, 2, 49, 50, 7,
	62, 2, 2, 50, 12, 3, 2, 2, 2, 51, 52, 7, 62, 2, 2, 52, 53, 7, 63, 2, 2,
	53, 14, 3, 2, 2, 2, 54, 55, 7, 42, 2, 2, 55, 16, 3, 2, 2, 2, 56, 57, 7,
	43, 2, 2, 57, 18, 3, 2, 2, 2, 58, 59, 7, 75, 2, 2, 59, 63, 7, 80, 2, 2,
	60, 61, 7, 107, 2, 2, 61, 63, 7, 112, 2, 2, 62, 58, 3, 2, 2, 2, 62, 60,
	3, 2, 2, 2, 63, 20, 3, 2, 2, 2, 64, 65, 7, 67, 2, 2, 65, 66, 7, 80, 2,
	2, 66, 71, 7, 70, 2, 2, 67, 68, 7, 99, 2, 2, 68, 69, 7, 112, 2, 2, 69,
	71, 7, 102, 2, 2, 70, 64, 3, 2, 2, 2, 70, 67, 3, 2, 2, 2, 71, 22, 3, 2,
	2, 2, 72, 73, 7, 68, 2, 2, 73, 74, 7, 71, 2, 2, 74, 75, 7, 86, 2, 2, 75,
	76, 7, 89, 2, 2, 76, 77, 7, 71, 2, 2, 77, 78, 7, 71, 2, 2, 78, 87, 7, 80,
	2, 2, 79, 80, 7, 100, 2, 2, 80, 81, 7, 103, 2, 2, 81, 82, 7, 118, 2, 2,
	82, 83, 7, 121, 2, 2, 83, 84, 7, 103, 2, 2, 84, 85, 7, 103, 2, 2, 85, 87,
	7, 112, 2, 2, 86, 72, 3, 2, 2, 2, 86, 79, 3, 2, 2, 2, 87, 24, 3, 2, 2,
	2, 88, 89, 7, 81, 2, 2, 89, 93, 7, 84, 2, 2, 90, 91, 7, 113, 2, 2, 91,
	93, 7, 116, 2, 2, 92, 88, 3, 2, 2, 2, 92, 90, 3, 2, 2, 2, 93, 26, 3, 2,
	2, 2, 94, 95, 7, 80, 2, 2, 95, 96, 7, 81, 2, 2, 96, 101, 7, 86, 2, 2, 97,
	98, 7, 112, 2, 2, 98, 99, 7, 113, 2, 2, 99, 101, 7, 118, 2, 2, 100, 94,
	3, 2, 2, 2, 100, 97, 3, 2, 2, 2, 101, 28, 3, 2, 2, 2, 102, 103, 7, 36,
	2, 2, 103, 30, 3, 2, 2, 2, 104, 105, 7, 46, 2, 2, 105, 32, 3, 2, 2, 2,
	106, 110, 9, 2, 2, 2, 107, 109, 9, 3, 2, 2, 108, 107, 3, 2, 2, 2, 109,
	112, 3, 2, 2, 2, 110, 108, 3, 2, 2, 2, 110, 111, 3, 2, 2, 2, 111, 114,
	3, 2, 2, 2, 112, 110, 3, 2, 2, 2, 113, 115, 9, 4, 2, 2, 114, 113, 3, 2,
	2, 2, 115, 116, 3, 2, 2, 2, 116, 114, 3, 2, 2, 2, 116, 117, 3, 2, 2, 2,
	117, 34, 3, 2, 2, 2, 118, 122, 9, 5, 2, 2, 119, 121, 9, 3, 2, 2, 120, 119,
	3, 2, 2, 2, 121, 124, 3, 2, 2, 2, 122, 120, 3, 2, 2, 2, 122, 123, 3, 2,
	2, 2, 123, 126, 3, 2, 2, 2, 124, 122, 3, 2, 2, 2, 125, 127, 9, 4, 2, 2,
	126, 125, 3, 2, 2, 2, 127, 128, 3, 2, 2, 2, 128, 126, 3, 2, 2, 2, 128,
	129, 3, 2, 2, 2, 129, 36, 3, 2, 2, 2, 130, 132, 9, 6, 2, 2, 131, 130, 3,
	2, 2, 2, 132, 133, 3, 2, 2, 2, 133, 131, 3, 2, 2, 2, 133, 134, 3, 2, 2,
	2, 134, 135, 3, 2, 2, 2, 135, 136, 8, 19, 2, 2, 136, 38, 3, 2, 2, 2, 13,
	2, 62, 70, 86, 92, 100, 110, 116, 122, 128, 133, 3, 8, 2, 2,
}

var lexerDeserializer = antlr.NewATNDeserializer(nil)
var lexerAtn = lexerDeserializer.DeserializeFromUInt16(serializedLexerAtn)

var lexerChannelNames = []string{
	"DEFAULT_TOKEN_CHANNEL", "HIDDEN",
}

var lexerModeNames = []string{
	"DEFAULT_MODE",
}

var lexerLiteralNames = []string{
	"", "'='", "'<>'", "'>'", "'>='", "'<'", "'<='", "'('", "')'", "", "",
	"", "", "", "'\"'", "','",
}

var lexerSymbolicNames = []string{
	"", "EQ", "NOT_EQ", "GT", "GTE", "LT", "LTE", "LPAREN", "RPAREN", "IN",
	"AND", "BETWEEN", "OR", "NOT", "QUOTE", "COMMA", "VALUE_HOLDER", "IDENT_HOLDER",
	"WHITESPACE",
}

var lexerRuleNames = []string{
	"EQ", "NOT_EQ", "GT", "GTE", "LT", "LTE", "LPAREN", "RPAREN", "IN", "AND",
	"BETWEEN", "OR", "NOT", "QUOTE", "COMMA", "VALUE_HOLDER", "IDENT_HOLDER",
	"WHITESPACE",
}

type DynamoLexer struct {
	*antlr.BaseLexer
	channelNames []string
	modeNames    []string
	// TODO: EOF string
}

var lexerDecisionToDFA = make([]*antlr.DFA, len(lexerAtn.DecisionToState))

func init() {
	for index, ds := range lexerAtn.DecisionToState {
		lexerDecisionToDFA[index] = antlr.NewDFA(ds, index)
	}
}

func NewDynamoLexer(input antlr.CharStream) *DynamoLexer {

	l := new(DynamoLexer)

	l.BaseLexer = antlr.NewBaseLexer(input)
	l.Interpreter = antlr.NewLexerATNSimulator(l, lexerAtn, lexerDecisionToDFA, antlr.NewPredictionContextCache())

	l.channelNames = lexerChannelNames
	l.modeNames = lexerModeNames
	l.RuleNames = lexerRuleNames
	l.LiteralNames = lexerLiteralNames
	l.SymbolicNames = lexerSymbolicNames
	l.GrammarFileName = "Dynamo.g4"
	// TODO: l.EOF = antlr.TokenEOF

	return l
}

// DynamoLexer tokens.
const (
	DynamoLexerEQ           = 1
	DynamoLexerNOT_EQ       = 2
	DynamoLexerGT           = 3
	DynamoLexerGTE          = 4
	DynamoLexerLT           = 5
	DynamoLexerLTE          = 6
	DynamoLexerLPAREN       = 7
	DynamoLexerRPAREN       = 8
	DynamoLexerIN           = 9
	DynamoLexerAND          = 10
	DynamoLexerBETWEEN      = 11
	DynamoLexerOR           = 12
	DynamoLexerNOT          = 13
	DynamoLexerQUOTE        = 14
	DynamoLexerCOMMA        = 15
	DynamoLexerVALUE_HOLDER = 16
	DynamoLexerIDENT_HOLDER = 17
	DynamoLexerWHITESPACE   = 18
)
