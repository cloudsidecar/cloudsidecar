// Generated from Dynamo.g4 by ANTLR 4.7.

package parser // Dynamo

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/antlr/antlr4/runtime/Go/antlr"
)

// Suppress unused import errors
var _ = fmt.Printf
var _ = reflect.Copy
var _ = strconv.Itoa

var parserATN = []uint16{
	3, 24715, 42794, 33075, 47597, 16764, 15335, 30598, 22884, 3, 20, 59, 4,
	2, 9, 2, 4, 3, 9, 3, 4, 4, 9, 4, 4, 5, 9, 5, 3, 2, 3, 2, 3, 2, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 7, 3, 30, 10, 3, 12, 3, 14, 3, 33, 11, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 5, 3, 42, 10, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 7, 3, 50, 10, 3, 12, 3, 14, 3, 53, 11, 3, 3, 4, 3, 4, 3, 5, 3,
	5, 3, 5, 2, 3, 4, 6, 2, 4, 6, 8, 2, 4, 3, 2, 18, 19, 4, 2, 3, 6, 8, 8,
	2, 61, 2, 10, 3, 2, 2, 2, 4, 41, 3, 2, 2, 2, 6, 54, 3, 2, 2, 2, 8, 56,
	3, 2, 2, 2, 10, 11, 5, 4, 3, 2, 11, 12, 7, 2, 2, 3, 12, 3, 3, 2, 2, 2,
	13, 14, 8, 3, 1, 2, 14, 15, 7, 19, 2, 2, 15, 16, 5, 8, 5, 2, 16, 17, 7,
	18, 2, 2, 17, 42, 3, 2, 2, 2, 18, 19, 7, 19, 2, 2, 19, 20, 7, 13, 2, 2,
	20, 21, 7, 18, 2, 2, 21, 22, 7, 12, 2, 2, 22, 42, 7, 18, 2, 2, 23, 24,
	7, 19, 2, 2, 24, 25, 7, 11, 2, 2, 25, 26, 7, 9, 2, 2, 26, 31, 7, 18, 2,
	2, 27, 28, 7, 17, 2, 2, 28, 30, 7, 18, 2, 2, 29, 27, 3, 2, 2, 2, 30, 33,
	3, 2, 2, 2, 31, 29, 3, 2, 2, 2, 31, 32, 3, 2, 2, 2, 32, 34, 3, 2, 2, 2,
	33, 31, 3, 2, 2, 2, 34, 42, 7, 10, 2, 2, 35, 36, 7, 15, 2, 2, 36, 42, 5,
	4, 3, 4, 37, 38, 7, 9, 2, 2, 38, 39, 5, 4, 3, 2, 39, 40, 7, 10, 2, 2, 40,
	42, 3, 2, 2, 2, 41, 13, 3, 2, 2, 2, 41, 18, 3, 2, 2, 2, 41, 23, 3, 2, 2,
	2, 41, 35, 3, 2, 2, 2, 41, 37, 3, 2, 2, 2, 42, 51, 3, 2, 2, 2, 43, 44,
	12, 6, 2, 2, 44, 45, 7, 12, 2, 2, 45, 50, 5, 4, 3, 7, 46, 47, 12, 5, 2,
	2, 47, 48, 7, 14, 2, 2, 48, 50, 5, 4, 3, 6, 49, 43, 3, 2, 2, 2, 49, 46,
	3, 2, 2, 2, 50, 53, 3, 2, 2, 2, 51, 49, 3, 2, 2, 2, 51, 52, 3, 2, 2, 2,
	52, 5, 3, 2, 2, 2, 53, 51, 3, 2, 2, 2, 54, 55, 9, 2, 2, 2, 55, 7, 3, 2,
	2, 2, 56, 57, 9, 3, 2, 2, 57, 9, 3, 2, 2, 2, 6, 31, 41, 49, 51,
}
var deserializer = antlr.NewATNDeserializer(nil)
var deserializedATN = deserializer.DeserializeFromUInt16(parserATN)

var literalNames = []string{
	"", "'='", "'<>'", "'>'", "'>='", "'<'", "'<='", "'('", "')'", "", "",
	"", "", "", "'\"'", "','",
}
var symbolicNames = []string{
	"", "EQ", "NOT_EQ", "GT", "GTE", "LT", "LTE", "LPAREN", "RPAREN", "IN",
	"AND", "BETWEEN", "OR", "NOT", "QUOTE", "COMMA", "VALUE_HOLDER", "IDENT_HOLDER",
	"WHITESPACE",
}

var ruleNames = []string{
	"start", "expression", "operand", "comparator",
}
var decisionToDFA = make([]*antlr.DFA, len(deserializedATN.DecisionToState))

func init() {
	for index, ds := range deserializedATN.DecisionToState {
		decisionToDFA[index] = antlr.NewDFA(ds, index)
	}
}

type DynamoParser struct {
	*antlr.BaseParser
}

func NewDynamoParser(input antlr.TokenStream) *DynamoParser {
	this := new(DynamoParser)

	this.BaseParser = antlr.NewBaseParser(input)

	this.Interpreter = antlr.NewParserATNSimulator(this, deserializedATN, decisionToDFA, antlr.NewPredictionContextCache())
	this.RuleNames = ruleNames
	this.LiteralNames = literalNames
	this.SymbolicNames = symbolicNames
	this.GrammarFileName = "Dynamo.g4"

	return this
}

// DynamoParser tokens.
const (
	DynamoParserEOF          = antlr.TokenEOF
	DynamoParserEQ           = 1
	DynamoParserNOT_EQ       = 2
	DynamoParserGT           = 3
	DynamoParserGTE          = 4
	DynamoParserLT           = 5
	DynamoParserLTE          = 6
	DynamoParserLPAREN       = 7
	DynamoParserRPAREN       = 8
	DynamoParserIN           = 9
	DynamoParserAND          = 10
	DynamoParserBETWEEN      = 11
	DynamoParserOR           = 12
	DynamoParserNOT          = 13
	DynamoParserQUOTE        = 14
	DynamoParserCOMMA        = 15
	DynamoParserVALUE_HOLDER = 16
	DynamoParserIDENT_HOLDER = 17
	DynamoParserWHITESPACE   = 18
)

// DynamoParser rules.
const (
	DynamoParserRULE_start      = 0
	DynamoParserRULE_expression = 1
	DynamoParserRULE_operand    = 2
	DynamoParserRULE_comparator = 3
)

// IStartContext is an interface to support dynamic dispatch.
type IStartContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsStartContext differentiates from other interfaces.
	IsStartContext()
}

type StartContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyStartContext() *StartContext {
	var p = new(StartContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = DynamoParserRULE_start
	return p
}

func (*StartContext) IsStartContext() {}

func NewStartContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *StartContext {
	var p = new(StartContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = DynamoParserRULE_start

	return p
}

func (s *StartContext) GetParser() antlr.Parser { return s.parser }

func (s *StartContext) Expression() IExpressionContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IExpressionContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *StartContext) EOF() antlr.TerminalNode {
	return s.GetToken(DynamoParserEOF, 0)
}

func (s *StartContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *StartContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *StartContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(DynamoListener); ok {
		listenerT.EnterStart(s)
	}
}

func (s *StartContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(DynamoListener); ok {
		listenerT.ExitStart(s)
	}
}

func (p *DynamoParser) Start() (localctx IStartContext) {
	localctx = NewStartContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 0, DynamoParserRULE_start)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(8)
		p.expression(0)
	}
	{
		p.SetState(9)
		p.Match(DynamoParserEOF)
	}

	return localctx
}

// IExpressionContext is an interface to support dynamic dispatch.
type IExpressionContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsExpressionContext differentiates from other interfaces.
	IsExpressionContext()
}

type ExpressionContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyExpressionContext() *ExpressionContext {
	var p = new(ExpressionContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = DynamoParserRULE_expression
	return p
}

func (*ExpressionContext) IsExpressionContext() {}

func NewExpressionContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ExpressionContext {
	var p = new(ExpressionContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = DynamoParserRULE_expression

	return p
}

func (s *ExpressionContext) GetParser() antlr.Parser { return s.parser }

func (s *ExpressionContext) IDENT_HOLDER() antlr.TerminalNode {
	return s.GetToken(DynamoParserIDENT_HOLDER, 0)
}

func (s *ExpressionContext) Comparator() IComparatorContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IComparatorContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IComparatorContext)
}

func (s *ExpressionContext) AllVALUE_HOLDER() []antlr.TerminalNode {
	return s.GetTokens(DynamoParserVALUE_HOLDER)
}

func (s *ExpressionContext) VALUE_HOLDER(i int) antlr.TerminalNode {
	return s.GetToken(DynamoParserVALUE_HOLDER, i)
}

func (s *ExpressionContext) BETWEEN() antlr.TerminalNode {
	return s.GetToken(DynamoParserBETWEEN, 0)
}

func (s *ExpressionContext) AND() antlr.TerminalNode {
	return s.GetToken(DynamoParserAND, 0)
}

func (s *ExpressionContext) IN() antlr.TerminalNode {
	return s.GetToken(DynamoParserIN, 0)
}

func (s *ExpressionContext) LPAREN() antlr.TerminalNode {
	return s.GetToken(DynamoParserLPAREN, 0)
}

func (s *ExpressionContext) RPAREN() antlr.TerminalNode {
	return s.GetToken(DynamoParserRPAREN, 0)
}

func (s *ExpressionContext) AllCOMMA() []antlr.TerminalNode {
	return s.GetTokens(DynamoParserCOMMA)
}

func (s *ExpressionContext) COMMA(i int) antlr.TerminalNode {
	return s.GetToken(DynamoParserCOMMA, i)
}

func (s *ExpressionContext) NOT() antlr.TerminalNode {
	return s.GetToken(DynamoParserNOT, 0)
}

func (s *ExpressionContext) AllExpression() []IExpressionContext {
	var ts = s.GetTypedRuleContexts(reflect.TypeOf((*IExpressionContext)(nil)).Elem())
	var tst = make([]IExpressionContext, len(ts))

	for i, t := range ts {
		if t != nil {
			tst[i] = t.(IExpressionContext)
		}
	}

	return tst
}

func (s *ExpressionContext) Expression(i int) IExpressionContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IExpressionContext)(nil)).Elem(), i)

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *ExpressionContext) OR() antlr.TerminalNode {
	return s.GetToken(DynamoParserOR, 0)
}

func (s *ExpressionContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ExpressionContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *ExpressionContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(DynamoListener); ok {
		listenerT.EnterExpression(s)
	}
}

func (s *ExpressionContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(DynamoListener); ok {
		listenerT.ExitExpression(s)
	}
}

func (p *DynamoParser) Expression() (localctx IExpressionContext) {
	return p.expression(0)
}

func (p *DynamoParser) expression(_p int) (localctx IExpressionContext) {
	var _parentctx antlr.ParserRuleContext = p.GetParserRuleContext()
	_parentState := p.GetState()
	localctx = NewExpressionContext(p, p.GetParserRuleContext(), _parentState)
	var _prevctx IExpressionContext = localctx
	var _ antlr.ParserRuleContext = _prevctx // TODO: To prevent unused variable warning.
	_startState := 2
	p.EnterRecursionRule(localctx, 2, DynamoParserRULE_expression, _p)
	var _la int

	defer func() {
		p.UnrollRecursionContexts(_parentctx)
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	var _alt int

	p.EnterOuterAlt(localctx, 1)
	p.SetState(39)
	p.GetErrorHandler().Sync(p)
	switch p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 1, p.GetParserRuleContext()) {
	case 1:
		{
			p.SetState(12)
			p.Match(DynamoParserIDENT_HOLDER)
		}
		{
			p.SetState(13)
			p.Comparator()
		}
		{
			p.SetState(14)
			p.Match(DynamoParserVALUE_HOLDER)
		}

	case 2:
		{
			p.SetState(16)
			p.Match(DynamoParserIDENT_HOLDER)
		}
		{
			p.SetState(17)
			p.Match(DynamoParserBETWEEN)
		}
		{
			p.SetState(18)
			p.Match(DynamoParserVALUE_HOLDER)
		}
		{
			p.SetState(19)
			p.Match(DynamoParserAND)
		}
		{
			p.SetState(20)
			p.Match(DynamoParserVALUE_HOLDER)
		}

	case 3:
		{
			p.SetState(21)
			p.Match(DynamoParserIDENT_HOLDER)
		}
		{
			p.SetState(22)
			p.Match(DynamoParserIN)
		}
		{
			p.SetState(23)
			p.Match(DynamoParserLPAREN)
		}
		{
			p.SetState(24)
			p.Match(DynamoParserVALUE_HOLDER)
		}
		p.SetState(29)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)

		for _la == DynamoParserCOMMA {
			{
				p.SetState(25)
				p.Match(DynamoParserCOMMA)
			}
			{
				p.SetState(26)
				p.Match(DynamoParserVALUE_HOLDER)
			}

			p.SetState(31)
			p.GetErrorHandler().Sync(p)
			_la = p.GetTokenStream().LA(1)
		}
		{
			p.SetState(32)
			p.Match(DynamoParserRPAREN)
		}

	case 4:
		{
			p.SetState(33)
			p.Match(DynamoParserNOT)
		}
		{
			p.SetState(34)
			p.expression(2)
		}

	case 5:
		{
			p.SetState(35)
			p.Match(DynamoParserLPAREN)
		}
		{
			p.SetState(36)
			p.expression(0)
		}
		{
			p.SetState(37)
			p.Match(DynamoParserRPAREN)
		}

	}
	p.GetParserRuleContext().SetStop(p.GetTokenStream().LT(-1))
	p.SetState(49)
	p.GetErrorHandler().Sync(p)
	_alt = p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 3, p.GetParserRuleContext())

	for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
		if _alt == 1 {
			if p.GetParseListeners() != nil {
				p.TriggerExitRuleEvent()
			}
			_prevctx = localctx
			p.SetState(47)
			p.GetErrorHandler().Sync(p)
			switch p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 2, p.GetParserRuleContext()) {
			case 1:
				localctx = NewExpressionContext(p, _parentctx, _parentState)
				p.PushNewRecursionContext(localctx, _startState, DynamoParserRULE_expression)
				p.SetState(41)

				if !(p.Precpred(p.GetParserRuleContext(), 4)) {
					panic(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 4)", ""))
				}
				{
					p.SetState(42)
					p.Match(DynamoParserAND)
				}
				{
					p.SetState(43)
					p.expression(5)
				}

			case 2:
				localctx = NewExpressionContext(p, _parentctx, _parentState)
				p.PushNewRecursionContext(localctx, _startState, DynamoParserRULE_expression)
				p.SetState(44)

				if !(p.Precpred(p.GetParserRuleContext(), 3)) {
					panic(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 3)", ""))
				}
				{
					p.SetState(45)
					p.Match(DynamoParserOR)
				}
				{
					p.SetState(46)
					p.expression(4)
				}

			}

		}
		p.SetState(51)
		p.GetErrorHandler().Sync(p)
		_alt = p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 3, p.GetParserRuleContext())
	}

	return localctx
}

// IOperandContext is an interface to support dynamic dispatch.
type IOperandContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsOperandContext differentiates from other interfaces.
	IsOperandContext()
}

type OperandContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyOperandContext() *OperandContext {
	var p = new(OperandContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = DynamoParserRULE_operand
	return p
}

func (*OperandContext) IsOperandContext() {}

func NewOperandContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *OperandContext {
	var p = new(OperandContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = DynamoParserRULE_operand

	return p
}

func (s *OperandContext) GetParser() antlr.Parser { return s.parser }

func (s *OperandContext) VALUE_HOLDER() antlr.TerminalNode {
	return s.GetToken(DynamoParserVALUE_HOLDER, 0)
}

func (s *OperandContext) IDENT_HOLDER() antlr.TerminalNode {
	return s.GetToken(DynamoParserIDENT_HOLDER, 0)
}

func (s *OperandContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *OperandContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *OperandContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(DynamoListener); ok {
		listenerT.EnterOperand(s)
	}
}

func (s *OperandContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(DynamoListener); ok {
		listenerT.ExitOperand(s)
	}
}

func (p *DynamoParser) Operand() (localctx IOperandContext) {
	localctx = NewOperandContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 4, DynamoParserRULE_operand)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	p.SetState(52)
	_la = p.GetTokenStream().LA(1)

	if !(_la == DynamoParserVALUE_HOLDER || _la == DynamoParserIDENT_HOLDER) {
		p.GetErrorHandler().RecoverInline(p)
	} else {
		p.GetErrorHandler().ReportMatch(p)
		p.Consume()
	}

	return localctx
}

// IComparatorContext is an interface to support dynamic dispatch.
type IComparatorContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsComparatorContext differentiates from other interfaces.
	IsComparatorContext()
}

type ComparatorContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyComparatorContext() *ComparatorContext {
	var p = new(ComparatorContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = DynamoParserRULE_comparator
	return p
}

func (*ComparatorContext) IsComparatorContext() {}

func NewComparatorContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ComparatorContext {
	var p = new(ComparatorContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = DynamoParserRULE_comparator

	return p
}

func (s *ComparatorContext) GetParser() antlr.Parser { return s.parser }

func (s *ComparatorContext) EQ() antlr.TerminalNode {
	return s.GetToken(DynamoParserEQ, 0)
}

func (s *ComparatorContext) NOT_EQ() antlr.TerminalNode {
	return s.GetToken(DynamoParserNOT_EQ, 0)
}

func (s *ComparatorContext) GT() antlr.TerminalNode {
	return s.GetToken(DynamoParserGT, 0)
}

func (s *ComparatorContext) GTE() antlr.TerminalNode {
	return s.GetToken(DynamoParserGTE, 0)
}

func (s *ComparatorContext) LTE() antlr.TerminalNode {
	return s.GetToken(DynamoParserLTE, 0)
}

func (s *ComparatorContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ComparatorContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *ComparatorContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(DynamoListener); ok {
		listenerT.EnterComparator(s)
	}
}

func (s *ComparatorContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(DynamoListener); ok {
		listenerT.ExitComparator(s)
	}
}

func (p *DynamoParser) Comparator() (localctx IComparatorContext) {
	localctx = NewComparatorContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 6, DynamoParserRULE_comparator)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	p.SetState(54)
	_la = p.GetTokenStream().LA(1)

	if !(((_la)&-(0x1f+1)) == 0 && ((1<<uint(_la))&((1<<DynamoParserEQ)|(1<<DynamoParserNOT_EQ)|(1<<DynamoParserGT)|(1<<DynamoParserGTE)|(1<<DynamoParserLTE))) != 0) {
		p.GetErrorHandler().RecoverInline(p)
	} else {
		p.GetErrorHandler().ReportMatch(p)
		p.Consume()
	}

	return localctx
}

func (p *DynamoParser) Sempred(localctx antlr.RuleContext, ruleIndex, predIndex int) bool {
	switch ruleIndex {
	case 1:
		var t *ExpressionContext = nil
		if localctx != nil {
			t = localctx.(*ExpressionContext)
		}
		return p.Expression_Sempred(t, predIndex)

	default:
		panic("No predicate with index: " + fmt.Sprint(ruleIndex))
	}
}

func (p *DynamoParser) Expression_Sempred(localctx antlr.RuleContext, predIndex int) bool {
	switch predIndex {
	case 0:
		return p.Precpred(p.GetParserRuleContext(), 4)

	case 1:
		return p.Precpred(p.GetParserRuleContext(), 3)

	default:
		panic("No predicate with index: " + fmt.Sprint(predIndex))
	}
}
