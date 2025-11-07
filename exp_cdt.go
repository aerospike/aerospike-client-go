package aerospike

// Module identifier for CDT expressions.
const expCdtMODULE int64 = 0

// Flags for CDT expression operations.
type ExpCdtFlags int64

const (
	// Identifier for CDT select expression.
	ExpCdtSelect ExpCdtFlags = 0xfe
)

// ExpSelectByPathOrdered creates a select by path expression using the new order format.
// This is the new version that packs arguments in a different order for compatibility.
func ExpSelectByPath(
	returnType ExpType,
	flag SelectFlag,
	bin *Expression,
	ctx ...*CDTContext,
) *Expression {
	args := []ExpressionArgument{
		expCdtSelectList{
			IntegerValue(ExpCdtSelect),
			cdtContextList(ctx),
			IntegerValue(flag),
		},
	}

	return expCdtRead(bin, returnType, args)
}

// ExpModifyByPathOrdered creates a modify by path expression using the new order format.
// This is the new version that packs arguments in a different order for compatibility.
func ExpModifyByPath(
	returnType ExpType,
	flag SelectFlag,
	bin *Expression,
	modifyExp *Expression,
	ctx ...*CDTContext,
) *Expression {
	args := []ExpressionArgument{
		expCdtModifyList{
			IntegerValue(ExpCdtSelect),
			cdtContextList(ctx),
			IntegerValue(flag | 4),
			modifyExp,
		},
	}

	return expCdtModify(bin, returnType, args)
}

// ===============================================
// Helper functions for encoding the expressions
// ===============================================

func expCdtRead(
	bin *Expression,
	returnType ExpType,
	arguments []ExpressionArgument,
) *Expression {
	flags := expCdtMODULE
	return &Expression{
		cmd:       &expOpCALL,
		val:       nil,
		bin:       bin,
		flags:     &flags,
		module:    &returnType,
		exps:      nil,
		arguments: arguments,
	}
}

func expCdtModify(
	bin *Expression,
	returnType ExpType,
	arguments []ExpressionArgument,
) *Expression {
	flags := expCdtMODULE | _MODIFY
	return &Expression{
		cmd:       &expOpCALL,
		val:       nil,
		bin:       bin,
		flags:     &flags,
		module:    &returnType,
		exps:      nil,
		arguments: arguments,
	}
}

type expCdtModifyList []ExpressionArgument

func (e expCdtModifyList) pack(buf BufferEx) (int, Error) {
	panic("expCdtModifyList.pack() should not be called directly")
}

type expCdtSelectList []ExpressionArgument

func (e expCdtSelectList) pack(buf BufferEx) (int, Error) {
	panic("expCdtSelectList.pack() should not be called directly")
}
