package postgresql

import (
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// PGFunction is the model for the database function
type PGFunction struct {
	Schema          string
	Name            string
	Returns         string
	Language        string
	Body            string
	Args            []PGFunctionArg
	SecurityDefiner bool
	Strict          bool
	Volatility      string
}

type PGFunctionArg struct {
	Name    string
	Type    string
	Mode    string
	Default string
}

func (pgFunction *PGFunction) FromResourceData(d *schema.ResourceData) error {

	if v, ok := d.GetOk(funcSchemaAttr); ok {
		pgFunction.Schema = v.(string)
	} else {
		pgFunction.Schema = "public"
	}

	pgFunction.Name = d.Get(funcNameAttr).(string)
	pgFunction.Returns = d.Get(funcReturnsAttr).(string)
	if v, ok := d.GetOk(funcLanguageAttr); ok {
		pgFunction.Language = v.(string)
	} else {
		pgFunction.Language = "plpgsql"
	}
	pgFunction.Body = normalizeFunctionBody(d.Get(funcBodyAttr).(string))
	pgFunction.Args = []PGFunctionArg{}

	if v, ok := d.GetOk(funcStrictAttr); ok {
		pgFunction.Strict = v.(bool)
	} else {
		pgFunction.Strict = false
	}
	if v, ok := d.GetOk(funcSecurityDefinerAttr); ok {
		pgFunction.SecurityDefiner = v.(bool)
	} else {
		pgFunction.SecurityDefiner = false
	}
	if v, ok := d.GetOk(funcVolatilityAttr); ok {
		pgFunction.Volatility = v.(string)
	} else {
		pgFunction.Volatility = defaultFunctionVolatility
	}

	// For the main returns if not provided
	argOutput := "void"

	if args, ok := d.GetOk(funcArgAttr); ok {
		args := args.([]interface{})

		for _, arg := range args {
			arg := arg.(map[string]interface{})

			var pgArg PGFunctionArg

			if v, ok := arg[funcArgModeAttr]; ok {
				pgArg.Mode = v.(string)
			}

			if v, ok := arg[funcArgNameAttr]; ok {
				pgArg.Name = v.(string)
			}

			pgArg.Type = arg[funcArgTypeAttr].(string)

			if v, ok := arg[funcArgDefaultAttr]; ok {
				pgArg.Default = v.(string)
			}

			// For the main returns if not provided
			if strings.ToUpper(pgArg.Mode) == "OUT" {
				argOutput = pgArg.Type
			}

			pgFunction.Args = append(pgFunction.Args, pgArg)
		}
	}

	if v, ok := d.GetOk(funcReturnsAttr); ok {
		pgFunction.Returns = v.(string)
	} else {
		pgFunction.Returns = argOutput
	}

	return nil
}

func (pgFunction *PGFunction) Parse(functionDefinition string) error {

	pgFunctionData := findStringSubmatchMap(
		`(?si)CREATE(\sOR\sREPLACE)?\sFUNCTION\s(?P<Schema>[^.]+)\.(?P<Name>[^(]+)\((?P<Args>.*)\).*RETURNS\s(?P<Returns>[^\n]+).*LANGUAGE\s(?P<Language>[^\n\s]+)\s*(?P<Volatility>(STABLE|IMMUTABLE)?)\s*(?P<Strict>(STRICT)?)\s*(?P<Security>(SECURITY DEFINER)?).*\$[a-zA-Z]*\$(?P<Body>.*)\$[a-zA-Z]*\$`,
		functionDefinition,
	)

	argsData := pgFunctionData["Args"]

	args := []PGFunctionArg{}

	if argsData != "" {
		rawArgs := strings.Split(argsData, ",")
		for i := 0; i < len(rawArgs); i++ {
			var arg PGFunctionArg
			err := arg.Parse(rawArgs[i])
			if err != nil {
				continue
			}
			args = append(args, arg)
		}
	}

	pgFunction.Schema = pgFunctionData["Schema"]
	pgFunction.Name = pgFunctionData["Name"]
	pgFunction.Returns = pgFunctionData["Returns"]
	pgFunction.Language = pgFunctionData["Language"]
	pgFunction.Body = pgFunctionData["Body"]
	pgFunction.Args = args
	pgFunction.SecurityDefiner = len(pgFunctionData["Security"]) > 0
	pgFunction.Strict = len(pgFunctionData["Strict"]) > 0
	if !pgFunction.Strict {
		// CRDB emits null-call behavior before LANGUAGE; scan the header (before first $) separately.
		header := functionDefinition
		if dollarIdx := strings.Index(functionDefinition, "$"); dollarIdx >= 0 {
			header = functionDefinition[:dollarIdx]
		}
		upperHeader := strings.ToUpper(header)
		pgFunction.Strict = strings.Contains(upperHeader, "RETURNS NULL ON NULL INPUT") ||
			strings.Contains(upperHeader, "STRICT")
	}
	if len(pgFunctionData["Volatility"]) == 0 {
		// CRDB emits IMMUTABLE/STABLE before LANGUAGE; scan the header (before first $) separately.
		pgFunction.Volatility = defaultFunctionVolatility
		dollarIdx := strings.Index(functionDefinition, "$")
		header := functionDefinition
		if dollarIdx >= 0 {
			header = functionDefinition[:dollarIdx]
		}
		upperHeader := strings.ToUpper(header)
		if strings.Contains(upperHeader, "IMMUTABLE") {
			pgFunction.Volatility = "IMMUTABLE"
		} else if strings.Contains(upperHeader, "STABLE") {
			pgFunction.Volatility = "STABLE"
		}
	} else {
		pgFunction.Volatility = pgFunctionData["Volatility"]
	}

	return nil
}

func (pgFunctionArg *PGFunctionArg) Parse(functionArgDefinition string) error {

	// Check if default exists
	argDefinitions := findStringSubmatchMap(`(?si)(?P<ArgData>.*)\sDEFAULT\s(?P<ArgDefault>.*)`, functionArgDefinition)

	argData := functionArgDefinition
	if len(argDefinitions) > 0 {
		argData = argDefinitions["ArgData"]
		pgFunctionArg.Default = argDefinitions["ArgDefault"]
	}

	pgFunctionArgData := findStringSubmatchMap(`(?si)((?P<Mode>IN|OUT|INOUT|VARIADIC)\s)?(?P<Name>[^\s]+)\s(?P<Type>.*)`, argData)

	pgFunctionArg.Name = pgFunctionArgData["Name"]
	pgFunctionArg.Type = pgFunctionArgData["Type"]
	pgFunctionArg.Mode = pgFunctionArgData["Mode"]
	if pgFunctionArg.Mode == "" {
		pgFunctionArg.Mode = "IN"
	}
	return nil
}

func normalizeFunctionBody(body string) string {
	newBodyMap := findStringSubmatchMap(`(?si).*\$[a-zA-Z]*\$\s(?P<Body>.*)\s\$[a-zA-Z]*\$.*`, body)
	if newBody, ok := newBodyMap["Body"]; ok {
		return newBody
	}
	return body
}

func normalizeFunctionBodyForCompare(body string) string {
	lines := strings.Split(body, "\n")
	var normalized []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			normalized = append(normalized, trimmed)
		}
	}
	return strings.Join(normalized, "\n")
}
