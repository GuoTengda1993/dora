package sqlz

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/json-iterator/go/extra"
)

const (
	SELECT = 1
	UPDATE = 2
	INSERT = 3
	DELETE = 4
)

type DBClient struct {
	DB           *sql.DB
	Info         *Info
	Tx           *sql.Tx
	AutoRollback bool
}

type Info struct {
	table     string
	sql       string
	args      []interface{}
	sType     int
	data      interface{}
	where     map[string]interface{}
	columns   []string
	limit     uint64
	offset    uint64
	orderBy   map[string]bool
	distinct  bool
}

func init() {
	extra.RegisterFuzzyDecoders()
}

func NewDB(driverName, username, password, host, dbName, charset string) (*DBClient, error) {
	if charset == "" {
		charset = "utf8"
	}
	ds := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=%s", username, password, host, dbName, charset)
	d, err := sql.Open(driverName, ds)
	if err != nil {
		return nil, err
	}
	err = d.Ping()
	if err != nil {
		defer d.Close()
		return nil, err
	}
	orderBy := make(map[string]bool)
	info := &Info{orderBy: orderBy}
	return &DBClient{DB: d, Info: info}, nil
}

func NewDbByConn(db *sql.DB) (*DBClient) {
	orderBy := make(map[string]bool)
	info := &Info{orderBy: orderBy}
	return &DBClient{DB: db, Info: info}
}

func (t *DBClient) SetConnAndTime(maxOpenConns, maxIdleConns int, maxLifeTime, maxIdleTime time.Duration) {
	t.DB.SetMaxOpenConns(maxOpenConns)
	t.DB.SetMaxIdleConns(maxIdleConns)
	t.DB.SetConnMaxLifetime(maxLifeTime)
	t.DB.SetConnMaxIdleTime(maxIdleTime)
}

func (t *DBClient) clear() {
	orderBy := make(map[string]bool)
	info := &Info{orderBy: orderBy}
	t.Info = info
}

func (t *DBClient) Begin(autoRollback bool) error {
	tx, err := t.DB.Begin()
	if err != nil {
		return err
	}
	t.Tx = tx
	t.AutoRollback = autoRollback
	return nil
}

func (t *DBClient) Commit() error {
	err := t.Tx.Commit()
	if err != nil {
		if rerr := t.Tx.Rollback(); rerr != nil {
			return fmt.Errorf("commit error: %s, and rollback err: %s", err.Error(), rerr.Error())
		}
		return fmt.Errorf("commit error: %w, rollback success", err)
	}
	return nil
}

func (t *DBClient) Close() {
	if t.DB != nil {
		t.DB.Close()
	}
}

func (t *DBClient) Table(table string) *DBClient {
	t.Info.table = table
	return t
}

func (t *DBClient) Select(columns []string, distinct bool) *DBClient {
	t.Info.sType = SELECT
	if len(columns) > 0 {
		t.Info.columns = columns
		t.Info.distinct = distinct
	} else {
		t.Info.distinct = false
	}
	return t
}

func (t *DBClient) Insert(data interface{}) *DBClient {
	t.Info.sType = INSERT
	t.Info.data = data
	return t
}

func (t *DBClient) Update(data interface{}) *DBClient {
	t.Info.sType = UPDATE
	t.Info.data = data
	return t
}

func (t *DBClient) Delete() *DBClient {
	t.Info.sType = DELETE
	return t
}

func (t *DBClient) Where(where map[string]interface{}) *DBClient {
	if len(where) > 0 {
		t.Info.where = where
	}
	return t
}

func (t *DBClient) Limit(limit uint64) *DBClient {
	t.Info.limit = limit
	return t
}

func (t *DBClient) Offset(offset uint64) *DBClient {
	t.Info.offset = offset
	return t
}

func (t *DBClient) OrderBy(col string, asc bool) *DBClient {
	t.Info.orderBy[col] = asc
	return t
}

func (t *DBClient) All(result interface{}) ([]map[string]interface{}, error) {
	defer t.clear()
	if t.Info.sType != SELECT {
		return nil, errors.New("not select sqlz")
	}
	var err error
	err = t.formatSQL()
	if err != nil {
		return nil, fmt.Errorf("formatSql error: %s", err.Error())
	}

	rows, err := t.DB.Query(t.Info.sql, t.Info.args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	n := len(columns)
	data := make([]map[string]interface{}, 0)
	values := make([]interface{}, n)
	valuePtrs := make([]interface{}, n)

	for rows.Next() {
		for i := 0; i < n; i++ {
			valuePtrs[i] = &values[i]
		}
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return data, err
		}
		d := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				d[col] = string(b)
				continue
			}
			d[col] = val
		}
		data = append(data, d)
	}
	if result != nil {
		err = t.setResult(data, result)
	}
	return data, err
}

func (t *DBClient) First(result interface{}) (map[string]interface{}, error) {
	t.Info.limit = 1
	var err error
	list, err := t.All(nil)
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return nil, nil
	}
	data := list[0]
	if result != nil {
		err = t.setResult(data, result)
	}
	return data, err
}

func (t *DBClient) setResult(data interface{}, result interface{}) error {
	var jsoni = jsoniter.ConfigCompatibleWithStandardLibrary
	js, err := jsoni.Marshal(data)
	if err != nil {
		return fmt.Errorf("make result error: %w", err)
	}
	return jsoni.Unmarshal(js, result)
}

func (t *DBClient) Do() (int64, error) {
	defer t.clear()
	if t.Info.sType == SELECT {
		return 0, errors.New("should not select sqlz")
	}
	err := t.formatSQL()
	if err != nil {
		return 0, fmt.Errorf("formatSql error: %s", err.Error())
	}
	var num int64

	if t.Tx != nil {
		res, err := t.Tx.Exec(t.Info.sql, t.Info.args...)
		if err != nil {
			if t.AutoRollback {
				if rerr := t.Tx.Rollback(); rerr != nil {
					return 0, fmt.Errorf("exec error: %s, and rollback err: %s", err.Error(), rerr.Error())
				}
			}
			return 0, fmt.Errorf("rollback:%v, exec error: %w", t.AutoRollback, err)
		}
		num, _ = res.RowsAffected()
	} else {
		res, err := t.DB.Exec(t.Info.sql, t.Info.args...)
		if err != nil {
			return 0, err
		}
		num, _ = res.RowsAffected()
	}
	return num, nil
}

func (t *DBClient) formatSQL() error {
	if t.Info.table == "" {
		return errors.New("table name empty")
	}
	if (t.Info.sType == INSERT || t.Info.sType == UPDATE) && t.Info.data == nil {
		return errors.New("insert/update no data")
	}
	switch t.Info.sType {
	case SELECT:
		cols := "*"
		if len(t.Info.columns) > 0 {
			var tmp []string
			for _, c := range t.Info.columns {
				tmp = append(tmp, fmt.Sprintf("`%s`", c))
			}
			cols = strings.Join(tmp, ",")
			if t.Info.distinct {
				cols = "DISTINCT " + cols
			}
		}
		t.Info.sql += fmt.Sprintf("SELECT %s FROM `%s`", cols, t.Info.table)
		t.buildWhere()
	case UPDATE:
		t.Info.sql += fmt.Sprintf("UPDATE `%s` SET", t.Info.table)
		t.buildData()
		t.buildWhere()
	case INSERT:
		t.Info.sql += fmt.Sprintf("INSERT INTO `%s`", t.Info.table)
		t.buildData()
	case DELETE:
		t.Info.sql += fmt.Sprintf("DELETE FROM `%s`", t.Info.table)
		t.buildWhere()
	}
	return nil
}

func (t *DBClient) buildWhere() {
	// WHERE
	if len(t.Info.where) > 0 {
		var list []string
		for k, v := range t.Info.where {
			ktmp := strings.Fields(k)
			mark := "="
			if len(ktmp) == 2 {
				mark = ktmp[1]
			}
			if strings.ToUpper(mark) == "IN" {
				inVal := v.([]interface{})
				if len(inVal) == 0 {
					continue
				}
				qs := strings.Repeat("?,", len(inVal))
				qs = qs[0: len(qs)-1]
				tmp := fmt.Sprintf("`%s` IN (%s)", ktmp[0], qs)
				t.Info.args = append(t.Info.args, inVal...)
			} else {
				tmp := fmt.Sprintf("`%s` %s ?", ktmp[0], mark)
				list = append(list, tmp)
				t.Info.args = append(t.Info.args, v)
			}
		}
		where := strings.Join(list, ",")
		t.Info.sql += fmt.Sprintf(" WHERE %s", where)
	}
	// ORDER BY
	if len(t.Info.orderBy) > 0 {
		tmp := []string{}
		for k, v := range t.Info.orderBy {
			var m string
			if v {
				m = "ASC"
			} else {
				m = "DESC"
			}
			tmp = append(tmp, fmt.Sprintf("`%s` %s", k, m))
		}
		oStr := strings.Join(tmp, ",")
		t.Info.sql += fmt.Sprintf(" ORDER BY %s", oStr)
	}
	// LIMIT
	if t.Info.limit > 0 {
		t.Info.sql += fmt.Sprintf(" LIMIT %d", t.Info.limit)
		if t.Info.offset > 0 {
			t.Info.sql += fmt.Sprintf(" OFFSET %d", t.Info.offset)
		}
	}
	return
}

func (t *DBClient) buildData() {
	dType := reflect.TypeOf(t.Info.data)
	dValue := reflect.ValueOf(t.Info.data)

	if dType.Kind() == reflect.Ptr {
		dValue = dValue.Elem()
		dType = dValue.Type()
	}

	var columnData []string
	var qm []string

	switch dType.Kind() {
	case reflect.Map:
		for key, val := range t.Info.data.(map[string]interface{}) {
			if t.Info.sType == INSERT {
				columnData = append(columnData, fmt.Sprintf("`%s`", key))
				qm = append(qm, "?")
			} else {
				columnData = append(columnData, fmt.Sprintf("`%s`=?", key))
			}
			t.Info.args = append(t.Info.args, val)
		}
	case reflect.Struct:
		for i := 0; i < dType.NumField(); i++ {
			field := dType.Field(i)
			key := field.Tag.Get("db")
			if key == "" {
				key = field.Tag.Get("json")
			}
			if key == "" {
				continue
			}

			if t.Info.sType == INSERT {
				columnData = append(columnData, fmt.Sprintf("`%s`", key))
				qm = append(qm, "?")
			} else {
				columnData = append(columnData, fmt.Sprintf("`%s`=?", key))
			}

			val := dValue.Field(i)
			switch val.Kind() {
			case reflect.String:
				t.Info.args = append(t.Info.args, val.String())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				t.Info.args = append(t.Info.args, val.Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				t.Info.args = append(t.Info.args, val.Uint())
			case reflect.Float32, reflect.Float64:
				t.Info.args = append(t.Info.args, val.Float())
			case reflect.Bool:
				t.Info.args = append(t.Info.args, val.Bool())
			default:
				t.Info.args = append(t.Info.args, val.String())
			}
		}
	default:
		panic("data not map or struct")
	}
	columnStr := strings.Join(columnData, ",")
	if t.Info.sType == INSERT {
		quotaStr := strings.Join(qm, ",")
		t.Info.sql += fmt.Sprintf(" (%s) VALUES (%s)", columnStr, quotaStr)
	} else {
		t.Info.sql += fmt.Sprintf(" %s", columnStr)
	}
	return
}
