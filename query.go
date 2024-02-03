// Copyright 2019 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package badgerhold

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"github.com/dgraph-io/badger/v4"
)

const (
	eq    = iota // ==
	ne           // !=
	gt           // >
	lt           // <
	ge           // >=
	le           // <=
	in           // in
	re           // regular expression
	fn           // func
	isnil        // test's for nil
	sw           // string starts with
	ew           // string ends with
	hk           // match map keys

	contains // slice only
	any      // slice only
	all      // slice only
)

// Key is shorthand for specifying a query to run again the Key in a badgerhold, simply returns ""
// Where(badgerhold.Key).Eq("testkey")
const Key = ""

// Query is a chained collection of criteria of which an object in the badgerhold needs to match to be returned
// an empty query matches against all records
type Query struct {
	index         string
	currentField  string
	fieldCriteria map[string][]*Criterion
	ors           []*Query

	badIndex bool
	dataType reflect.Type
	tx       *badger.Txn
	writable bool
	subquery bool
	bookmark *iterBookmark

	limit   int
	skip    int
	sort    []string
	reverse bool
}

// Slice turns a slice of any type into []interface{} by copying the slice values so it can be easily passed
// into queries that accept variadic parameters.
// Will panic if value is not a slice
func Slice(value interface{}) []interface{} {
	slc := reflect.ValueOf(value)

	s := make([]interface{}, slc.Len(), slc.Len()) // panics if value is not slice, array or map
	for i := range s {
		s[i] = slc.Index(i).Interface()
	}
	return s
}

// IsEmpty returns true if the query is an empty query
// an empty query matches against everything
func (q *Query) IsEmpty() bool {
	if q.index != "" {
		return false
	}
	if len(q.fieldCriteria) != 0 {
		return false
	}

	if q.ors != nil {
		return false
	}

	return true
}

// Criterion is an operator and a value that a given field needs to match on
type Criterion struct {
	query    *Query
	operator int
	value    interface{}
	values   []interface{}
}

func hasMatchFunc(criteria []*Criterion) bool {
	for _, c := range criteria {
		if c.operator == fn {
			return true
		}
	}
	return false
}

// Field allows for referencing a field in structure being compared
type Field string

// Where starts a query for specifying the criteria that an object in the badgerhold needs to match to
// be returned in a Find result
/*
Query API Example

	s.Find(badgerhold.Where("FieldName").Eq(value).And("AnotherField").Lt(AnotherValue).
		Or(badgerhold.Where("FieldName").Eq(anotherValue)

Since Gobs only encode exported fields, this will panic if you pass in a field with a lower case first letter
*/
func Where(field string) *Criterion {
	if !startsUpper(field) {
		panic("The first letter of a field in a badgerhold query must be upper-case")
	}

	return &Criterion{
		query: &Query{
			currentField:  field,
			fieldCriteria: make(map[string][]*Criterion),
		},
	}
}

// And creates another set of criterion the needs to apply to a query
func (q *Query) And(field string) *Criterion {
	if !startsUpper(field) {
		panic("The first letter of a field in a badgerhold query must be upper-case")
	}

	q.currentField = field
	return &Criterion{
		query: q,
	}
}

// Skip skips the number of records that match all the rest of the query criteria, and does not return them
// in the result set.  Setting skip multiple times, or to a negative value will panic
func (q *Query) Skip(amount int) *Query {
	if amount < 0 {
		panic("Skip must be set to a positive number")
	}

	if q.skip != 0 {
		panic(fmt.Sprintf("Skip has already been set to %d", q.skip))
	}

	q.skip = amount

	return q
}

// Limit sets the maximum number of records that can be returned by a query
// Setting Limit multiple times, or to a negative value will panic
func (q *Query) Limit(amount int) *Query {
	if amount < 0 {
		panic("Limit must be set to a positive number")
	}

	if q.limit != 0 {
		panic(fmt.Sprintf("Limit has already been set to %d", q.limit))
	}

	q.limit = amount

	return q
}

// Contains tests if the current field is a slice that contains the passed in value
func (c *Criterion) Contains(value interface{}) *Query {
	return c.op(contains, value)
}

// ContainsAll tests if the current field is a slice that contains all of the passed in values.  If any of the
// values are NOT contained in the slice, then no match is made
func (c *Criterion) ContainsAll(values ...interface{}) *Query {
	c.operator = all
	c.values = values

	q := c.query
	q.fieldCriteria[q.currentField] = append(q.fieldCriteria[q.currentField], c)

	return q
}

// ContainsAny tests if the current field is a slice that contains any of the passed in values.  If any of the
// values are contained in the slice, then a match is made
func (c *Criterion) ContainsAny(values ...interface{}) *Query {
	c.operator = any
	c.values = values

	q := c.query
	q.fieldCriteria[q.currentField] = append(q.fieldCriteria[q.currentField], c)

	return q
}

// HasKey tests if the field has a map key matching the passed in value
func (c *Criterion) HasKey(value interface{}) *Query {
	return c.op(hk, value)
}

// SortBy sorts the results by the given fields name
// Multiple fields can be used
func (q *Query) SortBy(fields ...string) *Query {
	for i := range fields {
		if fields[i] == Key {
			panic("Cannot sort by Key.")
		}
		var found bool
		for k := range q.sort {
			if q.sort[k] == fields[i] {
				found = true
				break
			}
		}
		if !found {
			q.sort = append(q.sort, fields[i])
		}
	}
	return q
}

// Reverse will reverse the current result set
// useful with SortBy
func (q *Query) Reverse() *Query {
	q.reverse = !q.reverse
	return q
}

// Index specifies the index to use when running this query
func (q *Query) Index(indexName string) *Query {
	if strings.Contains(indexName, ".") {
		// NOTE: I may reconsider this in the future
		panic("Nested indexes are not supported.  Only top level structures can be indexed")
	}
	q.index = indexName
	return q
}

func (q *Query) validateIndex(data interface{}) error {
	if q.index == "" {
		return nil
	}
	if q.dataType == nil {
		panic("Can't check for a valid index before query datatype is set")
	}

	if storer, ok := data.(Storer); ok {
		if _, ok = storer.Indexes()[q.index]; ok {
			return nil
		} else {
			return fmt.Errorf("The index %s does not exist", q.index)
		}
	}

	if _, ok := q.dataType.FieldByName(q.index); ok {
		return nil
	}

	for i := 0; i < q.dataType.NumField(); i++ {
		if tag := q.dataType.Field(i).Tag.Get(BadgerHoldIndexTag); tag == q.index {
			q.index = q.dataType.Field(i).Name
			return nil
		}
	}
	// no field name or custom index name found

	return fmt.Errorf("The index %s does not exist", q.index)
}

// Or creates another separate query that gets unioned with any other results in the query
// Or will panic if the query passed in contains a limit or skip value, as they are only
// allowed on top level queries
func (q *Query) Or(query *Query) *Query {
	if query.skip != 0 || query.limit != 0 {
		panic("Or'd queries cannot contain skip or limit values")
	}
	q.ors = append(q.ors, query)
	return q
}

// Matches returns whether the provided data matches the query.
// Will match all field criteria, including nested OR queries, but ignores limits, skips, sort orders, etc.
func (q *Query) Matches(s *Store, data interface{}) (bool, error) {
	var key []byte
	dataVal := reflect.ValueOf(data)
	for dataVal.Kind() == reflect.Ptr {
		dataVal = dataVal.Elem()
	}
	data = dataVal.Interface()
	storer := s.newStorer(data)
	if keyField, ok := getKeyField(dataVal.Type()); ok {
		fieldValue := dataVal.FieldByName(keyField.Name)
		var err error
		key, err = s.encodeKey(fieldValue.Interface(), storer.Type())
		if err != nil {
			return false, err
		}
	}
	return q.matches(s, key, dataVal, data)
}

func (q *Query) matches(s *Store, key []byte, value reflect.Value, data interface{}) (bool, error) {
	if result, err := q.matchesAllFields(s, key, value, data); result || err != nil {
		return result, err
	}
	for _, orQuery := range q.ors {
		if result, err := orQuery.matches(s, key, value, data); result || err != nil {
			return result, err
		}
	}
	return false, nil
}

func (q *Query) matchesAllFields(s *Store, key []byte, value reflect.Value, currentRow interface{}) (bool, error) {
	if q.IsEmpty() {
		return true, nil
	}

	for field, criteria := range q.fieldCriteria {
		if field == q.index && !q.badIndex && !hasMatchFunc(criteria) {
			// already handled by index Iterator
			continue
		}

		if field == Key {
			ok, err := s.matchesAllCriteria(criteria, key, true, q.dataType.Name(), currentRow)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}

			continue
		}

		fVal, err := fieldValue(value, field)
		if err != nil {
			return false, err
		}

		ok, err := s.matchesAllCriteria(criteria, fVal.Interface(), false, "", currentRow)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}

	return true, nil
}

func fieldValue(value reflect.Value, field string) (reflect.Value, error) {
	fields := strings.Split(field, ".")

	current := value
	for i := range fields {
		if current.Kind() == reflect.Ptr {
			current = current.Elem().FieldByName(fields[i])
		} else {
			current = current.FieldByName(fields[i])
		}
		if !current.IsValid() {
			return reflect.Value{}, fmt.Errorf("The field %s does not exist in the type %s", field, value)
		}
	}
	return current, nil
}

func (c *Criterion) op(op int, value interface{}) *Query {
	c.operator = op
	c.value = value

	q := c.query
	q.fieldCriteria[q.currentField] = append(q.fieldCriteria[q.currentField], c)

	return q
}

// Eq tests if the current field is Equal to the passed in value
func (c *Criterion) Eq(value interface{}) *Query {
	return c.op(eq, value)
}

// Ne test if the current field is Not Equal to the passed in value
func (c *Criterion) Ne(value interface{}) *Query {
	return c.op(ne, value)
}

// Gt test if the current field is Greater Than the passed in value
func (c *Criterion) Gt(value interface{}) *Query {
	return c.op(gt, value)
}

// Lt test if the current field is Less Than the passed in value
func (c *Criterion) Lt(value interface{}) *Query {
	return c.op(lt, value)
}

// Ge test if the current field is Greater Than or Equal To the passed in value
func (c *Criterion) Ge(value interface{}) *Query {
	return c.op(ge, value)
}

// Le test if the current field is Less Than or Equal To the passed in value
func (c *Criterion) Le(value interface{}) *Query {
	return c.op(le, value)
}

// In test if the current field is a member of the slice of values passed in
func (c *Criterion) In(values ...interface{}) *Query {
	c.operator = in
	c.values = values

	q := c.query
	q.fieldCriteria[q.currentField] = append(q.fieldCriteria[q.currentField], c)

	return q
}

// RegExp will test if a field matches against the regular expression
// The Field Value will be converted to string (%s) before testing
func (c *Criterion) RegExp(expression *regexp.Regexp) *Query {
	return c.op(re, expression)
}

// IsNil will test if a field is equal to nil
func (c *Criterion) IsNil() *Query {
	return c.op(isnil, nil)
}

// HasPrefix will test if a field starts with provided string
func (c *Criterion) HasPrefix(prefix string) *Query {
	return c.op(sw, prefix)
}

// HasSuffix will test if a field ends with provided string
func (c *Criterion) HasSuffix(suffix string) *Query {
	return c.op(ew, suffix)
}

// MatchFunc is a function used to test an arbitrary matching value in a query
type MatchFunc func(ra *RecordAccess) (bool, error)

// RecordAccess allows access to the current record, field or allows running a sub-query within a
// MatchFunc
type RecordAccess struct {
	record interface{}
	field  interface{}
	query  *Query
	store  *Store
}

// Field is the current field being queried
func (r *RecordAccess) Field() interface{} {
	return r.field
}

// Record is the complete record for a given row in badgerhold
func (r *RecordAccess) Record() interface{} {
	return r.record
}

// SubQuery allows you to run another query in the same transaction for each
// record in a parent query
func (r *RecordAccess) SubQuery(result interface{}, query *Query) error {
	query.subquery = true
	query.bookmark = r.query.bookmark
	return r.store.findQuery(r.query.tx, result, query)
}

// SubAggregateQuery allows you to run another aggregate query in the same transaction for each
// record in a parent query
func (r *RecordAccess) SubAggregateQuery(query *Query, groupBy ...string) ([]*AggregateResult, error) {
	query.subquery = true
	query.bookmark = r.query.bookmark
	return r.store.aggregateQuery(r.query.tx, r.record, query, groupBy...)
}

// MatchFunc will test if a field matches the passed in function
func (c *Criterion) MatchFunc(match MatchFunc) *Query {
	if c.query.currentField == Key {
		panic("Match func cannot be used against Keys, as the Key type is unknown at runtime, and there is " +
			"no value compare against")
	}

	return c.op(fn, match)
}

// test if the criterion passes with the passed in value
func (c *Criterion) test(s *Store, testValue interface{}, encoded bool, keyType string, currentRow interface{}) (bool, error) {
	var recordValue interface{}
	if encoded {
		if len(testValue.([]byte)) != 0 {
			if c.operator == in || c.operator == any || c.operator == all {
				// value is a slice of values, use c.values
				recordValue = newElemType(c.values[0])
			} else {
				recordValue = newElemType(c.value)
			}

			// used with keys
			if keyType != "" {
				err := s.decodeKey(testValue.([]byte), recordValue, keyType)
				if err != nil {
					return false, err
				}
			} else {
				err := s.decode(testValue.([]byte), recordValue)
				if err != nil {
					return false, err
				}
			}
		}
	} else {
		recordValue = testValue
	}

	switch c.operator {
	case in:
		for i := range c.values {
			result, err := c.compare(recordValue, c.values[i], currentRow)
			if err != nil {
				return false, err
			}
			if result == 0 {
				return true, nil
			}
		}

		return false, nil
	case re:
		return c.value.(*regexp.Regexp).Match([]byte(fmt.Sprintf("%s", recordValue))), nil
	case hk:
		v := reflect.ValueOf(recordValue).MapIndex(reflect.ValueOf(c.value))
		return !reflect.ValueOf(v).IsZero(), nil
	case fn:
		return c.value.(MatchFunc)(&RecordAccess{
			field:  recordValue,
			record: currentRow,
			query:  c.query,
			store:  s,
		})
	case isnil:
		return reflect.ValueOf(recordValue).IsNil(), nil
	case sw:
		return strings.HasPrefix(fmt.Sprintf("%s", getElem(recordValue)), fmt.Sprintf("%s", c.value)), nil
	case ew:
		return strings.HasSuffix(fmt.Sprintf("%s", getElem(recordValue)), fmt.Sprintf("%s", c.value)), nil
	case contains, any, all:
		slc := reflect.ValueOf(recordValue)
		kind := slc.Kind()
		if kind != reflect.Slice && kind != reflect.Array {
			// make slice containing recordValue
			for slc.Kind() == reflect.Ptr {
				slc = slc.Elem()
			}
			slc = reflect.Append(reflect.MakeSlice(reflect.SliceOf(slc.Type()), 0, 1), slc)
		}

		if c.operator == contains {
			for i := 0; i < slc.Len(); i++ {
				result, err := c.compare(slc.Index(i), c.value, currentRow)
				if err != nil {
					return false, err
				}
				if result == 0 {
					return true, nil
				}
			}
			return false, nil
		}

		if c.operator == any {
			for i := 0; i < slc.Len(); i++ {
				for k := range c.values {
					result, err := c.compare(slc.Index(i), c.values[k], currentRow)
					if err != nil {
						return false, err
					}
					if result == 0 {
						return true, nil
					}
				}
			}

			return false, nil
		}

		// c.operator == all {
		for k := range c.values {
			found := false
			for i := 0; i < slc.Len(); i++ {
				result, err := c.compare(slc.Index(i), c.values[k], currentRow)
				if err != nil {
					return false, err
				}
				if result == 0 {
					found = true
					break
				}
			}
			if !found {
				return false, nil
			}
		}

		return true, nil
	default:
		// comparison operators
		result, err := c.compare(recordValue, c.value, currentRow)
		if err != nil {
			return false, err
		}

		switch c.operator {
		case eq:
			return result == 0, nil
		case ne:
			return result != 0, nil
		case gt:
			return result > 0, nil
		case lt:
			return result < 0, nil
		case le:
			return result < 0 || result == 0, nil
		case ge:
			return result > 0 || result == 0, nil
		default:
			panic("invalid operator")
		}
	}
}

func (s *Store) matchesAllCriteria(criteria []*Criterion, value interface{}, encoded bool, keyType string,
	currentRow interface{}) (bool, error) {

	for i := range criteria {
		ok, err := criteria[i].test(s, value, encoded, keyType, currentRow)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}

	return true, nil
}

func startsUpper(str string) bool {
	if str == "" {
		return true
	}

	for _, r := range str {
		return unicode.IsUpper(r)
	}

	return false
}

func (q *Query) String() string {
	s := ""

	if q.index != "" {
		s += "Using Index [" + q.index + "] "
	}

	s += "Where "
	for field, criteria := range q.fieldCriteria {
		for i := range criteria {
			s += field + " " + criteria[i].String()
			s += "\n\tAND "
		}
	}

	// remove last AND
	s = s[:len(s)-6]

	for i := range q.ors {
		s += "\nOr " + q.ors[i].String()
	}

	return s
}

func (c *Criterion) String() string {
	s := ""
	switch c.operator {
	case eq:
		s += "=="
	case ne:
		s += "!="
	case gt:
		s += ">"
	case lt:
		s += "<"
	case le:
		s += "<="
	case ge:
		s += ">="
	case in:
		return "in " + fmt.Sprintf("%v", c.values)
	case re:
		s += "matches the regular expression"
	case fn:
		s += "matches the function"
	case isnil:
		return "is nil"
	case sw:
		return "starts with " + fmt.Sprintf("%+v", c.value)
	case ew:
		return "ends with " + fmt.Sprintf("%+v", c.value)
	default:
		panic("invalid operator")
	}
	return s + " " + fmt.Sprintf("%v", c.value)
}

type record struct {
	key   []byte
	value reflect.Value
}

func (s *Store) runQuery(tx *badger.Txn, dataType interface{}, query *Query, retrievedKeys KeyList, skip int,
	action func(r *record) error) error {
	storer := s.newStorer(dataType)

	tp := dataType

	for reflect.TypeOf(tp).Kind() == reflect.Ptr {
		tp = reflect.ValueOf(tp).Elem().Interface()
	}

	query.dataType = reflect.TypeOf(tp)
	err := query.validateIndex(dataType)
	if err != nil {
		return err
	}

	if len(query.sort) > 0 {
		return s.runQuerySort(tx, dataType, query, action)
	}

	iter := s.newIterator(tx, storer.Type(), query, query.bookmark)
	if (query.writable || query.subquery) && query.bookmark == nil {
		query.bookmark = iter.createBookmark()
	}

	defer func() {
		iter.Close()
		query.bookmark = nil
	}()

	if query.index != "" && query.badIndex {
		return fmt.Errorf("The index %s does not exist", query.index)
	}

	newKeys := make(KeyList, 0)

	limit := query.limit - len(retrievedKeys)

	for k, v := iter.Next(); k != nil; k, v = iter.Next() {
		if len(retrievedKeys) != 0 {
			// don't check this record if it's already been retrieved
			if retrievedKeys.in(k) {
				continue
			}
		}

		val := reflect.New(reflect.TypeOf(tp))

		err := s.decode(v, val.Interface())
		if err != nil {
			return err
		}

		query.tx = tx

		ok, err := query.matchesAllFields(s, k, val, val.Interface())
		if err != nil {
			return err
		}

		if ok {
			if skip > 0 {
				skip--
				continue
			}

			err = action(&record{
				key:   k,
				value: val,
			})
			if err != nil {
				return err
			}

			// track that this key's entry has been added to the result list
			newKeys.add(k)

			if query.limit != 0 {
				limit--
				if limit == 0 {
					break
				}
			}
		}

	}

	if iter.Error() != nil {
		return iter.Error()
	}

	if query.limit != 0 && limit == 0 {
		return nil
	}

	if len(query.ors) > 0 {
		iter.Close()
		for i := range newKeys {
			retrievedKeys.add(newKeys[i])
		}

		for i := range query.ors {
			err := s.runQuery(tx, tp, query.ors[i], retrievedKeys, skip, action)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// runQuerySort runs the query without sort, skip, or limit, then applies them to the entire result set
func (s *Store) runQuerySort(tx *badger.Txn, dataType interface{}, query *Query, action func(r *record) error) error {
	err := validateSortFields(query)
	if err != nil {
		return err
	}

	// Run query without sort, skip or limit
	// apply sort, skip and limit to entire dataset
	qCopy := *query
	qCopy.sort = nil
	qCopy.limit = 0
	qCopy.skip = 0

	var records []*record
	err = s.runQuery(tx, dataType, &qCopy, nil, 0,
		func(r *record) error {
			records = append(records, r)

			return nil
		})

	if err != nil {
		return err
	}

	sort.Slice(records, func(i, j int) bool {
		return sortFunction(query, records[i].value, records[j].value)
	})

	startIndex, endIndex := getSkipAndLimitRange(query, len(records))
	records = records[startIndex:endIndex]

	for i := range records {
		err = action(records[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func getSkipAndLimitRange(query *Query, recordsLen int) (startIndex, endIndex int) {
	if query.skip > recordsLen {
		return 0, 0
	}
	startIndex = query.skip
	endIndex = recordsLen
	limitIndex := query.limit + startIndex

	if query.limit > 0 && limitIndex <= recordsLen {
		endIndex = limitIndex
	}
	return startIndex, endIndex
}

func sortFunction(query *Query, first, second reflect.Value) bool {
	for _, field := range query.sort {
		val, err := fieldValue(reflect.Indirect(first), field)
		if err != nil {
			panic(err.Error()) // shouldn't happen due to field check above
		}
		value := val.Interface()

		val, err = fieldValue(reflect.Indirect(second), field)
		if err != nil {
			panic(err.Error()) // shouldn't happen due to field check above
		}

		other := val.Interface()

		if query.reverse {
			value, other = other, value
		}

		cmp, cerr := compare(value, other)
		if cerr != nil {
			// if for some reason there is an error on compare, fallback to a lexicographic compare
			valS := fmt.Sprintf("%s", value)
			otherS := fmt.Sprintf("%s", other)
			if valS < otherS {
				return true
			} else if valS == otherS {
				continue
			}
			return false
		}

		if cmp == -1 {
			return true
		} else if cmp == 0 {
			continue
		}
		return false
	}
	return false
}

func validateSortFields(query *Query) error {
	for _, field := range query.sort {
		fields := strings.Split(field, ".")

		current := query.dataType
		for i := range fields {
			var structField reflect.StructField
			found := false
			if current.Kind() == reflect.Ptr {
				structField, found = current.Elem().FieldByName(fields[i])
			} else {
				structField, found = current.FieldByName(fields[i])
			}

			if !found {
				return fmt.Errorf("The field %s does not exist in the type %s", field, query.dataType)
			}
			current = structField.Type
		}
	}
	return nil
}

func (s *Store) findQuery(tx *badger.Txn, result interface{}, query *Query) error {
	if query == nil {
		query = &Query{}
	}

	query.writable = false

	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr || resultVal.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}

	if isFindByIndexQuery(query) {
		return s.findByIndexQuery(tx, resultVal, query)
	}

	sliceVal := resultVal.Elem()

	elType := sliceVal.Type().Elem()

	tp := elType

	for tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}

	keyField, hasKeyField := getKeyField(tp)

	val := reflect.New(tp)

	err := s.runQuery(tx, val.Interface(), query, nil, query.skip,
		func(r *record) error {
			var rowValue reflect.Value

			if elType.Kind() == reflect.Ptr {
				rowValue = r.value
			} else {
				rowValue = r.value.Elem()
			}

			if hasKeyField {
				rowKey := rowValue
				for rowKey.Kind() == reflect.Ptr {
					rowKey = rowKey.Elem()
				}
				err := s.decodeKey(r.key, rowKey.FieldByName(keyField.Name).Addr().Interface(), tp.Name())
				if err != nil {
					return err
				}
			}

			sliceVal = reflect.Append(sliceVal, rowValue)

			return nil
		})

	if err != nil {
		return err
	}

	resultVal.Elem().Set(sliceVal.Slice(0, sliceVal.Len()))

	return nil
}

func isFindByIndexQuery(query *Query) bool {
	if query.index == "" || len(query.fieldCriteria) == 0 || len(query.fieldCriteria[query.index]) != 1 || len(query.ors) > 0 {
		return false
	}

	operator := query.fieldCriteria[query.index][0].operator
	return operator == eq || operator == in
}

func (s *Store) deleteQuery(tx *badger.Txn, dataType interface{}, query *Query) error {
	if query == nil {
		query = &Query{}
	}
	query.writable = true

	var records []*record

	err := s.runQuery(tx, dataType, query, nil, query.skip,
		func(r *record) error {
			records = append(records, r)

			return nil
		})

	if err != nil {
		return err
	}

	storer := s.newStorer(dataType)

	for i := range records {
		err := tx.Delete(records[i].key)
		if err != nil {
			return err
		}

		// remove any indexes
		err = s.indexDelete(storer, tx, records[i].key, records[i].value.Interface())
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) updateQuery(tx *badger.Txn, dataType interface{}, query *Query, update func(record interface{}) error) error {
	if query == nil {
		query = &Query{}
	}

	query.writable = true
	var records []*record

	err := s.runQuery(tx, dataType, query, nil, query.skip,
		func(r *record) error {
			records = append(records, r)

			return nil

		})

	if err != nil {
		return err
	}

	storer := s.newStorer(dataType)
	for i := range records {
		upVal := records[i].value.Interface()

		// delete any existing indexes bad on original value
		err := s.indexDelete(storer, tx, records[i].key, upVal)
		if err != nil {
			return err
		}

		err = update(upVal)
		if err != nil {
			return err
		}

		encVal, err := s.encode(upVal)
		if err != nil {
			return err
		}

		err = tx.Set(records[i].key, encVal)
		if err != nil {
			return err
		}

		// insert any new indexes
		err = s.indexAdd(storer, tx, records[i].key, upVal)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) aggregateQuery(tx *badger.Txn, dataType interface{}, query *Query, groupBy ...string) ([]*AggregateResult, error) {
	if query == nil {
		query = &Query{}
	}

	query.writable = false
	var result []*AggregateResult

	if len(groupBy) == 0 {
		result = append(result, &AggregateResult{})
	}

	err := s.runQuery(tx, dataType, query, nil, query.skip,
		func(r *record) error {
			if len(groupBy) == 0 {
				result[0].reduction = append(result[0].reduction, r.value)
				return nil
			}

			grouping := make([]reflect.Value, len(groupBy))

			for i := range groupBy {
				fVal := r.value.Elem().FieldByName(groupBy[i])
				if !fVal.IsValid() {
					return fmt.Errorf("The field %s does not exist in the type %s", groupBy[i],
						r.value.Type())
				}

				grouping[i] = fVal
			}

			var err error
			var c int
			var allEqual bool

			i := sort.Search(len(result), func(i int) bool {
				for j := range grouping {
					c, err = compare(result[i].group[j].Interface(), grouping[j].Interface())
					if err != nil {
						return true
					}
					if c != 0 {
						return c >= 0
					}
					// if group part is equal, compare the next group part
				}
				allEqual = true
				return true
			})

			if err != nil {
				return err
			}

			if i < len(result) {
				if allEqual {
					// group already exists, append results to reduction
					result[i].reduction = append(result[i].reduction, r.value)
					return nil
				}
			}

			// group  not found, create another grouping at i
			result = append(result, nil)
			copy(result[i+1:], result[i:])
			result[i] = &AggregateResult{
				group:     grouping,
				reduction: []reflect.Value{r.value},
			}

			return nil
		})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s *Store) findOneQuery(tx *badger.Txn, result interface{}, query *Query) error {
	if query == nil {
		query = &Query{}
	}
	originalLimit := query.limit

	query.limit = 1

	query.writable = false

	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr {
		panic("result argument must be an address")
	}

	elType := resultVal.Elem().Type()
	tp := elType

	for tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}

	keyField, hasKeyField := getKeyField(tp)

	val := reflect.New(tp)

	found := false

	err := s.runQuery(tx, val.Interface(), query, nil, query.skip,
		func(r *record) error {
			found = true
			var rowValue reflect.Value

			if elType.Kind() == reflect.Ptr {
				rowValue = r.value
			} else {
				rowValue = r.value.Elem()
			}

			if hasKeyField {
				rowKey := rowValue
				for rowKey.Kind() == reflect.Ptr {
					rowKey = rowKey.Elem()
				}
				err := s.decodeKey(r.key, rowKey.FieldByName(keyField.Name).Addr().Interface(), tp.Name())
				if err != nil {
					return err
				}
			}

			resultVal.Elem().Set(r.value.Elem())

			return nil
		})

	query.limit = originalLimit
	if err != nil {
		return err
	}

	if !found {
		return ErrNotFound
	}

	return nil
}

func (s *Store) forEach(tx *badger.Txn, query *Query, fn interface{}) error {
	if query == nil {
		query = &Query{}
	}

	fnVal := reflect.ValueOf(fn)
	argType := reflect.TypeOf(fn).In(0)

	if argType.Kind() == reflect.Ptr {
		argType = argType.Elem()
	}

	keyField, hasKeyField := getKeyField(argType)

	dataType := reflect.New(argType).Interface()
	storer := s.newStorer(dataType)

	return s.runQuery(tx, dataType, query, nil, query.skip, func(r *record) error {

		if hasKeyField {
			err := s.decodeKey(r.key, r.value.Elem().FieldByName(keyField.Name).Addr().Interface(), storer.Type())
			if err != nil {
				return err
			}
		}

		out := fnVal.Call([]reflect.Value{r.value})

		if len(out) != 1 {
			return fmt.Errorf("foreach function does not return an error")
		}

		if out[0].IsNil() {
			return nil
		}

		return out[0].Interface().(error)
	})
}

func (s *Store) countQuery(tx *badger.Txn, dataType interface{}, query *Query) (uint64, error) {
	if query == nil {
		query = &Query{}
	}

	var count uint64

	err := s.runQuery(tx, dataType, query, nil, query.skip,
		func(r *record) error {
			count++
			return nil
		})

	if err != nil {
		return 0, err
	}

	return count, nil
}

func (s *Store) findByIndexQuery(tx *badger.Txn, resultSlice reflect.Value, query *Query) (err error) {
	criteria := query.fieldCriteria[query.index][0]
	sliceType := resultSlice.Elem().Type()
	query.dataType = dereference(sliceType.Elem())

	data := reflect.New(query.dataType).Interface()
	storer := s.newStorer(data)
	err = query.validateIndex(data)
	if err != nil {
		return err
	}
	err = validateSortFields(query)
	if err != nil {
		return err
	}

	var keyList KeyList
	if criteria.operator == in {
		keyList, err = s.fetchIndexValues(tx, query, storer.Type(), criteria.values...)
	} else {
		keyList, err = s.fetchIndexValues(tx, query, storer.Type(), criteria.value)
	}
	if err != nil {
		return err
	}

	keyField, hasKeyField := getKeyField(query.dataType)

	slice := reflect.MakeSlice(sliceType, 0, len(keyList))
	for i := range keyList {
		item, err := tx.Get(keyList[i])
		if err == badger.ErrKeyNotFound {
			panic("inconsistency between keys stored in index and in Badger directly")
		}
		if err != nil {
			return err
		}

		newElement := reflect.New(query.dataType)
		err = item.Value(func(val []byte) error {
			return s.decode(val, newElement.Interface())
		})
		if err != nil {
			return err
		}
		if hasKeyField {
			err = s.setKeyField(keyList[i], newElement, keyField, storer.Type())
			if err != nil {
				return err
			}
		}

		ok, err := query.matchesAllFields(s, keyList[i], newElement, newElement.Interface())
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		if sliceType.Elem().Kind() != reflect.Ptr {
			newElement = newElement.Elem()
		}
		slice = reflect.Append(slice, newElement)
	}

	if len(query.sort) > 0 {
		sort.Slice(slice.Interface(), func(i, j int) bool {
			return sortFunction(query, slice.Index(i), slice.Index(j))
		})
	}

	startIndex, endIndex := getSkipAndLimitRange(query, slice.Len())
	slice = slice.Slice(startIndex, endIndex)

	resultSlice.Elem().Set(slice)
	return nil
}

func (s *Store) fetchIndexValues(tx *badger.Txn, query *Query, typeName string, indexKeys ...interface{}) (KeyList, error) {
	keyList := KeyList{}
	for i := range indexKeys {
		indexKeyValue, err := s.encode(indexKeys[i])
		if err != nil {
			return nil, err
		}

		indexKey := newIndexKey(typeName, query.index, indexKeyValue)

		item, err := tx.Get(indexKey)
		if err == badger.ErrKeyNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}

		indexValue := KeyList{}
		err = item.Value(func(val []byte) error {
			return s.decode(val, &indexValue)
		})
		if err != nil {
			return nil, err
		}
		keyList = append(keyList, indexValue...)
	}
	return keyList, nil
}

func (s *Store) setKeyField(data []byte, key reflect.Value, keyField reflect.StructField, typeName string) error {
	return s.decodeKey(data, key.Elem().FieldByName(keyField.Name).Addr().Interface(), typeName)
}

func dereference(value reflect.Type) reflect.Type {
	result := value
	for result.Kind() == reflect.Ptr {
		result = result.Elem()
	}
	return result
}
