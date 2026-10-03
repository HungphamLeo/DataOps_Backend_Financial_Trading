// REUSE 100%: market-maker-background/utils/logger.go
package utils

import (
	"log"
	"os"
)

func GetLogger(logFilePath string) (*log.Logger, *os.File, error) {
	file, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return nil, nil, err
	}
	logger := log.New(file, "", log.LstdFlags)
	return logger, file, nil
}
