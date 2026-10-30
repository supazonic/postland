package postland

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// DataType is a PostgreSQL column type plus the rule that converts raw source
// values (usually decoded JSON) into Go values the driver can store.
//
// Coercion does not care where a value came from: "2024-01-01" from an API,
// a CSV file or another database is handled the same way.
type DataType struct {
	SQL         string
	coerce      func(any) (any, error)
	IsJSON      bool
	emptyIsNull bool // treat "" as NULL (true for non-text types)
}

// Coerce converts v for this type. nil stays nil.
func (t DataType) Coerce(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	if s, ok := v.(string); ok && t.emptyIsNull && strings.TrimSpace(s) == "" {
		return nil, nil
	}
	if n, ok := v.(json.Number); ok {
		v = string(n)
	}
	out, err := t.coerce(v)
	if err != nil {
		return nil, fmt.Errorf("cannot convert %#v to %s: %w", v, t.SQL, err)
	}
	return out, nil
}

func (t DataType) String() string { return t.SQL }

var (
	Text        = DataType{SQL: "TEXT", coerce: toText}
	SmallInt    = DataType{SQL: "SMALLINT", coerce: boundedInt(16), emptyIsNull: true}
	Integer     = DataType{SQL: "INTEGER", coerce: boundedInt(32), emptyIsNull: true}
	BigInt      = DataType{SQL: "BIGINT", coerce: boundedInt(64), emptyIsNull: true}
	Real        = DataType{SQL: "REAL", coerce: toFloat32, emptyIsNull: true}
	Double      = DataType{SQL: "DOUBLE PRECISION", coerce: toFloat64, emptyIsNull: true}
	Numeric     = DataType{SQL: "NUMERIC", coerce: toNumeric(-1), emptyIsNull: true}
	Boolean     = DataType{SQL: "BOOLEAN", coerce: toBool, emptyIsNull: true}
	Date        = DataType{SQL: "DATE", coerce: toDate, emptyIsNull: true}
	Time        = DataType{SQL: "TIME", coerce: toTimeOfDay, emptyIsNull: true}
	Timestamp   = DataType{SQL: "TIMESTAMP", coerce: toTimestamp, emptyIsNull: true}
	TimestampTZ = DataType{SQL: "TIMESTAMPTZ", coerce: toTimestampTZ, emptyIsNull: true}
	JSON        = DataType{SQL: "JSON", coerce: toJSON, IsJSON: true, emptyIsNull: true}
	JSONB       = DataType{SQL: "JSONB", coerce: toJSON, IsJSON: true, emptyIsNull: true}
	UUID        = DataType{SQL: "UUID", coerce: toUUID, emptyIsNull: true}
	Bytea       = DataType{SQL: "BYTEA", coerce: toBytes}
)

// Varchar is VARCHAR(n); longer values are rejected rather than truncated.
func Varchar(n int) DataType {
	return DataType{SQL: fmt.Sprintf("VARCHAR(%d)", n), coerce: func(v any) (any, error) {
		s, err := toText(v)
		if err != nil {
			return nil, err
		}
		if len([]rune(s.(string))) > n {
			return nil, fmt.Errorf("longer than %d characters", n)
		}
		return s, nil
	}}
}

// NumericPS is NUMERIC(precision, scale); values are rounded to scale digits.
func NumericPS(precision, scale int) DataType {
	return DataType{SQL: fmt.Sprintf("NUMERIC(%d,%d)", precision, scale), coerce: toNumeric(scale), emptyIsNull: true}
}

// ArrayOf is a PostgreSQL array column, e.g. ArrayOf(Text) is TEXT[].
func ArrayOf(item DataType) DataType {
	if item.IsJSON {
		panic("postland: use a JSON/JSONB column instead of an array of JSON")
	}
	return DataType{SQL: item.SQL + "[]", emptyIsNull: true, coerce: func(v any) (any, error) {
		if s, ok := v.(string); ok {
			if decoded, err := toJSON(s); err == nil {
				v = decoded
			}
		}
		rv := reflect.ValueOf(v)
		if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
			v, rv = []any{v}, reflect.ValueOf([]any{v})
		}
		out := make([]any, rv.Len())
		for i := range out {
			c, err := item.Coerce(rv.Index(i).Interface())
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			out[i] = c
		}
		return out, nil
	}}
}

// Custom is an escape hatch for any other PostgreSQL type (INET, CITEXT...).
// A nil coerce passes values through unchanged.
func Custom(sql string, coerce func(any) (any, error)) DataType {
	if coerce == nil {
		coerce = func(v any) (any, error) { return v, nil }
	}
	return DataType{SQL: sql, coerce: coerce, emptyIsNull: true}
}

// ---------------------------------------------------------------------------
// coercion helpers
// ---------------------------------------------------------------------------

func toText(v any) (any, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	case map[string]any, []any:
		b, err := json.Marshal(x)
		return string(b), err
	case time.Time:
		return x.Format(time.RFC3339Nano), nil
	case fmt.Stringer:
		return x.String(), nil
	}
	return fmt.Sprint(v), nil
}

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case bool:
		if x {
			return 1, nil
		}
		return 0, nil
	case int:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		if x > math.MaxInt64 {
			return 0, errors.New("out of range")
		}
		return int64(x), nil
	case float32:
		return toInt64(float64(x))
	case float64:
		if x != math.Trunc(x) || math.IsInf(x, 0) || math.IsNaN(x) {
			return 0, errors.New("not a whole number")
		}
		if x > math.MaxInt64 || x < math.MinInt64 {
			return 0, errors.New("out of range")
		}
		return int64(x), nil
	case string:
		s := strings.TrimSpace(x)
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i, nil
		}
		r, ok := new(big.Rat).SetString(s) // "12.0", "1e3"
		if !ok {
			return 0, errors.New("not a number")
		}
		if !r.IsInt() {
			return 0, errors.New("not a whole number")
		}
		if !r.Num().IsInt64() {
			return 0, errors.New("out of range")
		}
		return r.Num().Int64(), nil
	}
	return 0, fmt.Errorf("unsupported type %T", v)
}

func boundedInt(bits int) func(any) (any, error) {
	lo, hi := int64(-1)<<(bits-1), int64(1)<<(bits-1)-1
	if bits == 64 {
		lo, hi = math.MinInt64, math.MaxInt64
	}
	return func(v any) (any, error) {
		i, err := toInt64(v)
		if err != nil {
			return nil, err
		}
		if i < lo || i > hi {
			return nil, fmt.Errorf("%d out of range for %d-bit integer", i, bits)
		}
		switch bits {
		case 16:
			return int16(i), nil
		case 32:
			return int32(i), nil
		}
		return i, nil
	}
}

func toFloat64(v any) (any, error) {
	switch x := v.(type) {
	case bool:
		return nil, errors.New("refusing to convert a boolean")
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case string:
		return strconv.ParseFloat(strings.TrimSpace(x), 64)
	}
	i, err := toInt64(v)
	return float64(i), err
}

func toFloat32(v any) (any, error) {
	f, err := toFloat64(v)
	if err != nil {
		return nil, err
	}
	return float32(f.(float64)), nil
}

// toNumeric returns a canonical decimal string (exact; no float rounding).
// scale < 0 keeps every digit.
func toNumeric(scale int) func(any) (any, error) {
	return func(v any) (any, error) {
		var s string
		switch x := v.(type) {
		case bool:
			return nil, errors.New("refusing to convert a boolean")
		case string:
			s = strings.TrimSpace(x)
		case float64:
			s = strconv.FormatFloat(x, 'f', -1, 64)
		case float32:
			s = strconv.FormatFloat(float64(x), 'f', -1, 32)
		default:
			i, err := toInt64(v)
			if err != nil {
				return nil, err
			}
			s = strconv.FormatInt(i, 10)
		}
		r, ok := new(big.Rat).SetString(s)
		if !ok {
			return nil, errors.New("not a number")
		}
		if scale >= 0 {
			return r.FloatString(scale), nil
		}
		if r.IsInt() {
			return r.Num().String(), nil
		}
		// keep as many digits as the input had
		return strings.TrimRight(r.FloatString(64), "0"), nil
	}
}

func toBool(v any) (any, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case string:
		switch strings.ToLower(strings.TrimSpace(x)) {
		case "true", "t", "yes", "y", "1", "on":
			return true, nil
		case "false", "f", "no", "n", "0", "off":
			return false, nil
		}
		return nil, errors.New("not a boolean")
	}
	i, err := toInt64(v)
	if err != nil || (i != 0 && i != 1) {
		return nil, errors.New("not a boolean")
	}
	return i == 1, nil
}

var timeLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999-0700",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999-0700",
	"2006-01-02T15:04Z07:00",
	"2006-01-02T15:04",
	"2006-01-02",
	time.RFC1123Z,
	time.RFC1123,
}

// parseTime accepts time.Time, ISO-8601/RFC-3339 strings and epoch seconds
// or milliseconds. Strings without an offset are read as UTC.
func parseTime(v any) (time.Time, error) {
	switch x := v.(type) {
	case time.Time:
		return x, nil
	case string:
		s := strings.TrimSpace(x)
		if _, err := strconv.ParseFloat(s, 64); err == nil {
			return parseTime(mustFloat(s))
		}
		for _, layout := range timeLayouts {
			if t, err := time.Parse(layout, s); err == nil {
				return t, nil
			}
		}
		return time.Time{}, errors.New("unrecognised date/time format")
	}
	f, err := toFloat64(v)
	if err != nil {
		return time.Time{}, err
	}
	secs := f.(float64)
	if math.Abs(secs) > 1e11 { // looks like epoch milliseconds
		secs /= 1000
	}
	whole, frac := math.Modf(secs)
	return time.Unix(int64(whole), int64(frac*1e9)).UTC(), nil
}

func mustFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

// toTimestampTZ: naive values are taken to be UTC.
func toTimestampTZ(v any) (any, error) {
	t, err := parseTime(v)
	if err != nil {
		return nil, err
	}
	return t.UTC(), nil
}

// toTimestamp: zoned values are converted to UTC wall-clock time.
func toTimestamp(v any) (any, error) {
	t, err := parseTime(v)
	if err != nil {
		return nil, err
	}
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), u.Hour(), u.Minute(), u.Second(), u.Nanosecond(), time.UTC), nil
}

func toDate(v any) (any, error) {
	t, err := parseTime(v)
	if err != nil {
		return nil, err
	}
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), nil
}

func toTimeOfDay(v any) (any, error) {
	if t, ok := v.(time.Time); ok {
		return t.Format("15:04:05.999999"), nil
	}
	s := strings.TrimSpace(fmt.Sprint(v))
	for _, layout := range []string{"15:04:05.999999999", "15:04"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.Format("15:04:05.999999"), nil
		}
	}
	return nil, errors.New("not a time of day")
}

// toJSON decodes strings that hold serialised JSON, so they land as
// structured JSON rather than as a JSON string literal.
func toJSON(v any) (any, error) {
	if s, ok := v.(string); ok {
		t := strings.TrimSpace(s)
		if strings.HasPrefix(t, "{") || strings.HasPrefix(t, "[") || t == "null" || t == "true" || t == "false" {
			var out any
			d := json.NewDecoder(strings.NewReader(t))
			d.UseNumber()
			if err := d.Decode(&out); err == nil {
				return out, nil
			}
		}
	}
	return v, nil
}

func toUUID(v any) (any, error) {
	s := strings.ToLower(strings.TrimSpace(fmt.Sprint(v)))
	s = strings.TrimSuffix(strings.TrimPrefix(s, "{"), "}")
	hex := strings.ReplaceAll(s, "-", "")
	if len(hex) != 32 {
		return nil, errors.New("not a UUID")
	}
	for _, c := range hex {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return nil, errors.New("not a UUID")
		}
	}
	return hex[0:8] + "-" + hex[8:12] + "-" + hex[12:16] + "-" + hex[16:20] + "-" + hex[20:], nil
}

func toBytes(v any) (any, error) {
	switch x := v.(type) {
	case []byte:
		return x, nil
	case string:
		return []byte(x), nil
	}
	return []byte(fmt.Sprint(v)), nil
}
