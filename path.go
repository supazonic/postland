package postland

import (
	"reflect"
	"strconv"
	"strings"
)

// Lookup resolves a path such as "customer.email", "items[0].sku",
// "items.0.sku" or "$" (the value itself) against nested maps and slices,
// as produced by encoding/json. ok is false if any step is missing.
func Lookup(v any, path string) (result any, ok bool) {
	cur := v
	for _, tok := range splitPath(path) {
		if cur == nil {
			return nil, false
		}
		switch c := cur.(type) {
		case map[string]any:
			cur, ok = c[tok]
			if !ok {
				return nil, false
			}
		case []any:
			i, err := strconv.Atoi(tok)
			if err != nil || i < 0 || i >= len(c) {
				return nil, false
			}
			cur = c[i]
		default:
			next, found := lookupReflect(cur, tok)
			if !found {
				return nil, false
			}
			cur = next
		}
	}
	return cur, true
}

// splitPath turns "a.b[0].c" into ["a", "b", "0", "c"].
func splitPath(path string) []string {
	path = strings.TrimPrefix(path, "$")
	path = strings.TrimPrefix(path, ".")
	if path == "" {
		return nil
	}
	path = strings.ReplaceAll(path, "[", ".")
	path = strings.ReplaceAll(path, "]", "")
	parts := strings.Split(path, ".")
	out := parts[:0]
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// lookupReflect supports typed maps, slices and struct fields so records do
// not have to be map[string]any (e.g. rows scanned into structs by a future source).
func lookupReflect(v any, tok string) (any, bool) {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			return nil, false
		}
		rv = rv.Elem()
	}
	switch rv.Kind() {
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return nil, false
		}
		val := rv.MapIndex(reflect.ValueOf(tok).Convert(rv.Type().Key()))
		if !val.IsValid() {
			return nil, false
		}
		return val.Interface(), true
	case reflect.Slice, reflect.Array:
		i, err := strconv.Atoi(tok)
		if err != nil || i < 0 || i >= rv.Len() {
			return nil, false
		}
		return rv.Index(i).Interface(), true
	case reflect.Struct:
		f := rv.FieldByName(tok)
		if !f.IsValid() || !f.CanInterface() {
			return nil, false
		}
		return f.Interface(), true
	}
	return nil, false
}
