// REUSE 100%: market-maker-background/utils/time.go
package utils

import "time"

const TIME_FORMAT = "02/01/2006 15:04:05"

func GetTimeNow() string {
	return time.Now().Format(TIME_FORMAT)
}
