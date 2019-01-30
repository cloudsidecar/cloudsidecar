package antlr

import (
	"fmt"
	"github.com/antlr/antlr4/runtime/Go/antlr"
	antlr_parser "sidecar/dynamo_parser"
	"sidecar/logging"
)

type Listener struct {
	*antlr_parser.BaseDynamoListener
	identifiers  map[string]string
	values       map[string]interface {}
	Filters      []string
	FilterValues []interface {}
	InFilters    map[string][]interface {}
}

func (s *Listener) ExitExpression(ctx *antlr_parser.ExpressionContext) {
	if compare := ctx.Comparator(); compare != nil {
		ident := s.identifiers[ctx.IDENT_HOLDER().GetText()]
		filter := fmt.Sprint(ident, " ", compare.GetText())
		s.Filters = append(s.Filters, filter)
		value := s.values[ctx.VALUE_HOLDER(0).GetText()]
		s.FilterValues = append(s.FilterValues, value)
		//vars := ctx.GetTokens(antlr_parser.DynamoParserVALUE_HOLDER)
	} else if between := ctx.BETWEEN(); between != nil {
		ident := s.identifiers[ctx.IDENT_HOLDER().GetText()]
		lValue := s.values[ctx.VALUE_HOLDER(0).GetText()]
		rValue := s.values[ctx.VALUE_HOLDER(1).GetText()]
		lFilter := fmt.Sprint(ident, " ", ">=")
		rFilter := fmt.Sprint(ident, " ", "<=")
		s.Filters = append(s.Filters, lFilter)
		s.FilterValues = append(s.FilterValues, lValue)
		s.Filters = append(s.Filters, rFilter)
		s.FilterValues = append(s.FilterValues, rValue)
	} else if in := ctx.IN(); in != nil {
		ident := s.identifiers[ctx.IDENT_HOLDER().GetText()]
		values := ctx.AllVALUE_HOLDER()
		valuesAsString := make([]interface {}, len(values))
		for i, value := range values {
			valuesAsString[i] = s.values[value.GetText()]
		}
		s.InFilters[ident] = valuesAsString
	}
}

func Lex(input string, identifiers map[string]string, values map[string]interface {}) Listener {
	logging.Log.Debug("Parsing ", input, identifiers, values)
	is := antlr.NewInputStream(input)

	// Create the Lexer
	lexer := antlr_parser.NewDynamoLexer(is)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := antlr_parser.NewDynamoParser(stream)

	listener := Listener{
		identifiers: identifiers,
		values:      values,
		InFilters:   make(map[string][]interface {}),
	}
	antlr.ParseTreeWalkerDefault.Walk(&listener, p.Start())
	logging.Log.Debug("", listener.Filters)
	logging.Log.Debug("", listener.FilterValues)
	logging.Log.Debug("", listener.InFilters)
	return listener

}
