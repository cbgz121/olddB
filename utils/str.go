package utils

import "strconv"

//将float64类型转换为string类型
func Float64ToStr(val float64) string {
	return strconv.FormatFloat(val, 'f', -1, 64)
}

//将string类型转换为float64类型
func StrToFloat64(val string) (float64, error) {
	return strconv.ParseFloat(val, 64)
}
