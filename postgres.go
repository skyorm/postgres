package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
	"github.com/skyorm/skyorm"
)

// New returns new postgres provider.
func New(dsn string, log skyorm.Logger) (skyorm.Provider, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = skyorm.DefaultLogger
	}
	return &provider{db, log}, nil
}

type provider struct {
	db     *sql.DB
	logger skyorm.Logger
}

func (p *provider) Put(ctx context.Context, models ...skyorm.Model) error {
	for _, m := range models {
		isSerial := isPkEmpty(m.OrmPk())
		vl := len(m.OrmVals())
		if isSerial {
			vl--
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING %s",
			m.OrmStore().Name(),
			buildQueryProperties(m.OrmProps(), isSerial),
			buildInsertPlaceholders(vl),
			m.OrmPkProp().Name(),
		)
		values := m.OrmVals()
		if isSerial {
			values = make([]interface{}, 0, vl)
			for i, v := range m.OrmVals() {
				if m.OrmProps()[i].IsPk() {
					continue
				}
				values = append(values, v)
			}
		}
		p.logLn("PUT QUERY: %s", query)
		row := p.db.QueryRowContext(ctx, query, values...)
		if row.Err() != nil {
			return row.Err()
		}
		if err := row.Scan(m.OrmPkPointer()); err != nil {
			return err
		}
	}
	return nil
}

func (p *provider) Populate(ctx context.Context, model skyorm.Model, pk interface{}) error {
	query, args := buildWhere(
		skyorm.Eq(model.OrmPkProp(), pk),
		"SELECT %s FROM %s",
		nil,
		buildQueryProperties(model.OrmProps(), false),
		model.OrmStore().Name(),
	)
	p.logLn("GET QUERY: " + query)
	return p.db.QueryRowContext(ctx, query, args...).Scan(model.OrmPointers()...)
}

func (p *provider) Find(ctx context.Context, store skyorm.Store, condition skyorm.Cond, limit, offset int) ([]skyorm.Model, error) {
	query, args := buildWhere(condition,
		"SELECT %s FROM %s",
		nil,
		buildQueryProperties(store.Props(), false),
		store.Name(),
	)
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)
	}
	p.logLn("FIND QUERY: %s", query)
	res, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = res.Close()
	}()
	l := make([]skyorm.Model, 0)
	for res.Next() {
		m := store.Model()
		if err = res.Scan(m.OrmPointers()...); err != nil {
			return nil, err
		}
		l = append(l, m)
	}
	return l, nil
}

func (p *provider) Update(ctx context.Context, store skyorm.Store, condition skyorm.Cond, values ...skyorm.Val) error {
	cursor, updateString, updateValues := buildUpdateProps(values...)
	p.logLn("%d %s", cursor, updateString)
	query, args := buildWhere(
		condition,
		"UPDATE %s SET %s",
		&cursor,
		store.Name(),
		updateString,
	)
	for _, arg := range args {
		updateValues = append(updateValues, arg)
	}
	p.logLn("UPDATE QUERY: %s", query)
	if _, err := p.db.ExecContext(ctx, query, updateValues...); err != nil {
		return err
	}
	return nil
}

func (p *provider) Delete(ctx context.Context, store skyorm.Store, condition skyorm.Cond) error {
	query, args := buildWhere(condition, "DELETE FROM %s", nil, store.Name())
	p.logLn("DELETE QUERY: %s", query)
	if _, err := p.db.ExecContext(ctx, query, args...); err != nil {
		return err
	}
	return nil
}

func (p *provider) Count(ctx context.Context, store skyorm.Store, condition skyorm.Cond) (int64, error) {
	query, args := buildWhere(
		condition,
		"SELECT COUNT(%s) AS cnt FROM %s",
		nil,
		store.Pk().Name(),
		store.Name(),
	)
	row := p.db.QueryRowContext(ctx, query, args...)
	if err := row.Err(); err != nil {
		return 0, err
	}
	var cnt int64
	if err := row.Scan(&cnt); err != nil {
		return 0, err
	}
	return cnt, nil
}

func (p *provider) ErrNotFound() error {
	return sql.ErrNoRows
}

func (p *provider) logLn(format string, v ...interface{}) {
	p.logger.Printf(format+"\n", v...)
}

var (
	emptyInterfaceSlice = make([]interface{}, 0, 1)
)

func isPkEmpty(pk interface{}) bool {
	switch pk.(type) {
	case string:
		return pk.(string) == ""
	case int, int8, int16, int32, int64:
		return pk.(int64) == 0
	case uint, uint8, uint16, uint32, uint64:
		return pk.(uint64) == 0
	case float32, float64:
		return pk.(float64) == 0
	}
	return false
}

func buildWhere(condition skyorm.Cond, query string, n *int, queryValues ...interface{}) (string, []interface{}) {
	condWhere, condValues := parseCond(condition, n)
	if condWhere != "" {
		query += " WHERE %s"
		queryValues = append(queryValues, condWhere)
	}
	return fmt.Sprintf(query, queryValues...), condValues
}

func buildUpdateProps(values ...skyorm.Val) (int, string, []interface{}) {
	var (
		ls = make([]string, len(values))
		lv = make([]interface{}, len(values))
		i  = 0
		v  skyorm.Val
	)
	for i, v = range values {
		ls[i] = v.Prop().Name() + " = $" + strconv.Itoa(i+1)
		lv[i] = v.Val()
	}
	return i + 2, strings.Join(ls, ", "), lv
}

func buildQueryProperties(properties []skyorm.Prop, isSerial bool) string {
	ln := len(properties)
	if isSerial {
		ln--
	}
	l := make([]string, ln)
	c := 0
	for _, p := range properties {
		if isSerial && p.IsPk() {
			continue
		}
		l[c] = p.Name()
		c++
	}
	return strings.Join(l, ", ")
}

func buildInsertPlaceholders(n int) string {
	l := make([]string, n)
	for i := 1; i <= n; i++ {
		l[i-1] = "$" + strconv.Itoa(i)
	}
	return strings.Join(l, ", ")
}

func newN() *int {
	n := 1
	return &n
}

func parseCond(c skyorm.Cond, n *int) (string, []interface{}) {
	if c == nil {
		return "", emptyInterfaceSlice
	}
	if n == nil {
		n = newN()
	}
	if c.Type() == skyorm.CondTypeAnd || c.Type() == skyorm.CondTypeOr {
		sl := make([]string, 0)
		vl := make([]interface{}, 0)
		sep := " AND "
		if c.Type() == skyorm.CondTypeOr {
			sep = " OR "
		}
		s, v := parseCondChildren(c.Children(), sep, n)
		sl = append(sl, s)
		vl = append(vl, v...)
		if len(vl) == 0 {
			return "", vl
		}
		return strings.Join(sl, sep), vl
	} else {
		s, v := parseRegularCond(c, n)
		return s, []interface{}{v}
	}
}

func parseRegularCond(c skyorm.Cond, n *int) (string, interface{}) {
	*n++
	switch c.Type() {
	case skyorm.CondTypeEq:
		return c.Prop().Name() + " = $" + strconv.Itoa(*n-1), c.Val()
	case skyorm.CondTypeNeq:
		return c.Prop().Name() + " <> $" + strconv.Itoa(*n-1), c.Val()
	case skyorm.CondTypeLt:
		return c.Prop().Name() + " < $" + strconv.Itoa(*n-1), c.Val()
	case skyorm.CondTypeLte:
		return c.Prop().Name() + " <= $" + strconv.Itoa(*n-1), c.Val()
	case skyorm.CondTypeGt:
		return c.Prop().Name() + " > $" + strconv.Itoa(*n-1), c.Val()
	case skyorm.CondTypeGte:
		return c.Prop().Name() + " >= $" + strconv.Itoa(*n-1), c.Val()
	}
	return "", nil
}

func parseCondChildren(children []skyorm.Cond, sep string, n *int) (string, []interface{}) {
	cl := make([]string, len(children))
	vl := make([]interface{}, 0)
	for i, child := range children {
		c, v := parseCond(child, n)
		cl[i] = c
		vl = append(vl, v...)
	}
	return "(" + strings.Join(cl, sep) + ")", vl
}
