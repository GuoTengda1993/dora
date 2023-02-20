package sqlz

import (
	_ "github.com/go-sql-driver/mysql"
	"testing"
)

func TestSQL(t *testing.T) {
	username := ""
	password := ""
	host := ""
	dbName := ""
	d, err := NewDB("mysql", username, password, host, dbName, "utf8")
	if err != nil {
		t.Errorf("Conn Error: %s", err.Error())
		t.Fail()
		return
	}
	defer d.Close()
	type Name struct {
		ID   uint   `db:"id"`
		Name string `db:"name"`
		Age  int    `db:"age"`
	}
	name := Name{
		ID:   1,
		Name: "abc",
		Age:  10,
	}
	_, err = d.Table("test").Insert(name).Do()
	if err != nil {
		t.Errorf("Inert Error: %s", err.Error())
		t.Fail()
		return
	}

	result := &Name{}
	where := map[string]interface{}{
		"name": "abc",
	}
	_, err = d.Table("test").Select(nil, false).Where(where).First(result)
	if err != nil {
		t.Errorf("Inert Error: %s", err.Error())
		t.Fail()
		return
	}
	t.Logf("select result: %+v", result)

	data := map[string]interface{}{
		"age": 20,
	}
	n, err := d.Table("test").Update(data).Where(where).Do()
	if err != nil {
		t.Errorf("Inert Error: %s", err.Error())
		t.Fail()
		return
	}
	t.Logf("update affected: %d", n)

	_, err = d.Table("test").Delete().Where(where).Do()
	if err != nil {
		t.Errorf("Inert Error: %s", err.Error())
		t.Fail()
		return
	}
	t.Logf("delete succ")
}
