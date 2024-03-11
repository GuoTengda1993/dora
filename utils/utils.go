package utils

import (
    "reflect"
)

// ToAnySlice return a slice type of []any
// no matter of the type of input ([]uint64/string)
func ToAnySlice(slice any) []any {
    s := reflect.ValueOf(slice)
    if s.Kind() != reflect.Slice {
      panic("Given a non-slice type")
    }
    if s.IsNil(){
      return nil
    }
    r := make([]any, s.Len())
    for i:=0; i<s.Len(); i++ {
      r[i] = s.Index(i).Interface()
    }
    return r
}
