package otgorm

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/jinzhu/gorm"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/trace"
	"reflect"
	"regexp"
	"runtime"
	"time"
	"unicode"
)

//Attributes that may or may not be added to a span based on Options used
const (
	TableKey = label.Key("gorm.table") //The table the GORM query is acting upon
	QueryKey = label.Key("gorm.query") //The GORM query itself
)

type callbacks struct {
	//Allow otgorm to create root spans in the absence of a parent span.
	//Default is to not allow root spans.
	allowRoot bool

	//Record the DB query as a KeyValue onto the span where the DB is called
	query bool

	//Record the table that the sql query is acting on
	table bool

	//List of default attributes to include onto the span for DB calls
	defaultAttributes []label.KeyValue

	//tracer creates spans. This is required
	tracer trace.Tracer

	//List of default options spans will start with
	spanOptions []trace.SpanOption
}

//Gorm scope keys for passing around context and span within the DB scope
var (
	contextScopeKey = "_otContext"
	spanScopeKey    = "_otSpan"
)

// Option allows for managing otgorm configuration using functional options.
type Option interface {
	apply(c *callbacks)
}

// OptionFunc converts a regular function to an Option if it's definition is compatible.
type OptionFunc func(c *callbacks)

func (fn OptionFunc) apply(c *callbacks) {
	fn(c)
}

//WithSpanOptions configures the db callback functions with an additional set of
//trace.StartOptions which will be applied to each new span
func WithSpanOptions(opts ...trace.SpanOption) OptionFunc {
	return func(c *callbacks) {
		c.spanOptions = opts
	}
}

//WithTracer configures the tracer to use when starting spans. Otherwise
//the global tracer is used with a default name
func WithTracer(tracer trace.Tracer) OptionFunc {
	return func(c *callbacks) {
		c.tracer = tracer
	}
}

// AllowRoot allows creating root spans in the absence of existing spans.
type AllowRoot bool

func (a AllowRoot) apply(c *callbacks) {
	c.allowRoot = bool(a)
}

// Query allows recording the sql queries in spans.
type Query bool

func (q Query) apply(c *callbacks) {
	c.query = bool(q)
}

//Table allows for recording the table affected by sql queries in spans
type Table bool

func (t Table) apply(c *callbacks) {
	c.table = bool(t)
}

// DefaultAttributes sets attributes to each span.
type DefaultAttributes []label.KeyValue

func (d DefaultAttributes) apply(c *callbacks) {
	c.defaultAttributes = []label.KeyValue(d)
}

// RegisterCallbacks registers the necessary callbacks in Gorm's hook system for instrumentation with OpenTelemetry Spans.
func RegisterCallbacks(db *gorm.DB, opts ...Option) {
	c := &callbacks{
		defaultAttributes: []label.KeyValue{},
	}
	defaultOpts := []Option{
		// Default to the global tracer if not configured
		WithTracer(otel.GetTracerProvider().Tracer("otgorm")),
		WithSpanOptions(trace.WithSpanKind(trace.SpanKindInternal)),
	}

	for _, opt := range append(defaultOpts, opts...) {
		opt.apply(c)
	}

	db.Callback().Create().Before("gorm:create").Register("before_create", c.beforeCreate)
	db.Callback().Create().After("gorm:create").Register("after_create", c.afterCreate)
	db.Callback().Query().Before("gorm:query").Register("before_query", c.beforeQuery)
	db.Callback().Query().After("gorm:query").Register("after_query", c.afterQuery)
	db.Callback().Update().Before("gorm:update").Register("before_update", c.beforeUpdate)
	db.Callback().Update().After("gorm:update").Register("after_update", c.afterUpdate)
	db.Callback().Delete().Before("gorm:delete").Register("before_delete", c.beforeDelete)
	db.Callback().Delete().After("gorm:delete").Register("after_delete", c.afterDelete)
	db.Callback().RowQuery().Before("gorm:row_query").Register("before_row_query", c.beforeRowQuery)
	db.Callback().RowQuery().After("gorm:row_query").Register("after_row_query", c.afterRowQuery)
}

func (c *callbacks) before(scope *gorm.Scope, operation string) {
	rctx, _ := scope.Get(contextScopeKey)
	ctx, ok := rctx.(context.Context)
	if !ok || ctx == nil {
		ctx = context.Background()
	}

	ctx = c.startTrace(ctx, scope, operation)

	scope.Set(contextScopeKey, ctx)
}

func (c *callbacks) after(scope *gorm.Scope) {
	c.endTrace(scope)
}

func (c *callbacks) startTrace(ctx context.Context, scope *gorm.Scope, operation string) context.Context {
	//Start with configured span options
	opts := append([]trace.SpanOption{}, c.spanOptions...)

	// There's no context but we are ok with root spans
	if ctx == nil {
		ctx = context.Background()
	}

	//If there's no parent span and we don't allow root spans, return context
	parentSpan := trace.SpanFromContext(ctx)
	if parentSpan == nil && !c.allowRoot {
		return ctx
	}

	var span trace.Span

	if parentSpan == nil {
		ctx, span = c.tracer.Start(
			context.Background(),
			fmt.Sprintf("gorm:%s", operation),
			opts...,
		)
	} else {
		ctx, span = c.tracer.Start(ctx, fmt.Sprintf("gorm:%s", operation), opts...)
	}

	scope.Set(spanScopeKey, span)

	return ctx
}

func (c *callbacks) endTrace(scope *gorm.Scope) {
	rspan, ok := scope.Get(spanScopeKey)
	if !ok {
		return
	}

	span, ok := rspan.(trace.Span)
	if !ok {
		return
	}

	//Apply span attributes
	attributes := c.defaultAttributes

	if c.table {
		attributes = append(attributes, TableKey.String(scope.TableName()))
	}

	if c.query {
		attributes = append(attributes, QueryKey.String(LogFormatter(scope.SQL, scope.SQLVars)))
	}
	attributes = append(attributes, label.String("path", fileWithLineNum()))
	span.SetAttributes(attributes...)

	//Set StatusCode if there are any issues
	code := codes.Ok
	msg := ""
	if scope.HasError() {
		err := scope.DB().Error
		code = codes.Error
		if gorm.IsRecordNotFoundError(err) {
			msg = "gorm:NotFound"
		} else {
			msg = "gorm:Unknown"
		}

	}

	span.SetStatus(code, msg)

	//End Span
	span.End()
}

func (c *callbacks) beforeCreate(scope *gorm.Scope)   { c.before(scope, "create") }
func (c *callbacks) afterCreate(scope *gorm.Scope)    { c.after(scope) }
func (c *callbacks) beforeQuery(scope *gorm.Scope)    { c.before(scope, "query") }
func (c *callbacks) afterQuery(scope *gorm.Scope)     { c.after(scope) }
func (c *callbacks) beforeUpdate(scope *gorm.Scope)   { c.before(scope, "update") }
func (c *callbacks) afterUpdate(scope *gorm.Scope)    { c.after(scope) }
func (c *callbacks) beforeDelete(scope *gorm.Scope)   { c.before(scope, "delete") }
func (c *callbacks) afterDelete(scope *gorm.Scope)    { c.after(scope) }
func (c *callbacks) beforeRowQuery(scope *gorm.Scope) { c.before(scope, "row_query") }
func (c *callbacks) afterRowQuery(scope *gorm.Scope)  { c.after(scope) }

func fileWithLineNum() string {
	_, file, line, ok := runtime.Caller(6)
	if ok {
		return fmt.Sprintf("%v:%v", file, line)
	}
	return ""
}

var (
	sqlRegexp                = regexp.MustCompile(`\?`)
	numericPlaceHolderRegexp = regexp.MustCompile(`\$\d+`)
	goSrcRegexp              = regexp.MustCompile(`golang/ocgorm(@.*)?/.*.go`)
	goTestRegexp             = regexp.MustCompile(`jinzhu/gorm(@.*)?/.*test.go`)
)

func isPrintable(s string) bool {
	for _, r := range s {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

var LogFormatter = func(values ...interface{}) string {
	var (
		sql             string
		formattedValues []string
	)

	for _, value := range values[1].([]interface{}) {
		indirectValue := reflect.Indirect(reflect.ValueOf(value))
		if indirectValue.IsValid() {
			value = indirectValue.Interface()
			if t, ok := value.(time.Time); ok {
				if t.IsZero() {
					formattedValues = append(formattedValues, fmt.Sprintf("'%v'", "0000-00-00 00:00:00"))
				} else {
					formattedValues = append(formattedValues, fmt.Sprintf("'%v'", t.Format("2006-01-02 15:04:05")))
				}
			} else if b, ok := value.([]byte); ok {
				if str := string(b); isPrintable(str) {
					formattedValues = append(formattedValues, fmt.Sprintf("'%v'", str))
				} else {
					formattedValues = append(formattedValues, "'<binary>'")
				}
			} else if r, ok := value.(driver.Valuer); ok {
				if value, err := r.Value(); err == nil && value != nil {
					formattedValues = append(formattedValues, fmt.Sprintf("'%v'", value))
				} else {
					formattedValues = append(formattedValues, "NULL")
				}
			} else {
				switch value.(type) {
				case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
					formattedValues = append(formattedValues, fmt.Sprintf("%v", value))
				default:
					formattedValues = append(formattedValues, fmt.Sprintf("'%v'", value))
				}
			}
		} else {
			formattedValues = append(formattedValues, "NULL")
		}
	}

	// differentiate between $n placeholders or else treat like ?
	if numericPlaceHolderRegexp.MatchString(values[0].(string)) {
		sql = values[0].(string)
		for index, value := range formattedValues {
			placeholder := fmt.Sprintf(`\$%d([^\d]|$)`, index+1)
			sql = regexp.MustCompile(placeholder).ReplaceAllString(sql, value+"$1")
		}
	} else {
		formattedValuesLength := len(formattedValues)
		for index, value := range sqlRegexp.Split(values[0].(string), -1) {
			sql += value
			if index < formattedValuesLength {
				sql += formattedValues[index]
			}
		}
	}

	return sql
}
