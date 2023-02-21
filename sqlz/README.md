# SQLz

### Connect
```go
package main

import (
	"github.com/GuoTengda1993/dora/sqlz"
)

func main(){
	db, err := sqlz.NewDB("mysql", "user", "pw", "127.0.0.1:3306", "database1", "utf8")
	if err != nil {
		panic(err)
	}
	// do something...
	db.Close()
}
```

### Select

```go
package main

import (
	"fmt"
	"github.com/GuoTengda1993/dora/sqlz"
)

type User struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

type UserList struct {
	Data []User
}

const TABLE = "users"

func main() {
	db, err := sqlz.NewDB("mysql", "user", "pw", "127.0.0.1:3306", "database1", "utf8")
	if err != nil {
		panic(err)
	}

	x := &User{}
	w1 := map[string]interface{}{
		"name": "test",
	}
	_, err = db.Table(TABLE).Select(nil).Where(w1).First(x)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("x: %+v", x)
	
	xList := &UserList{}
	w2 := map[string]interface{}{
		"age >": 18,
	}
	_, err = db.Table(TABLE).Select(nil).Where(w2).All(xList.Data)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("x: %+v", xList.Data)

	db.Close()
}
```