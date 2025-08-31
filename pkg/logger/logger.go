package logger

import (
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
)

var logger *logrus.Logger

func init() {
	logger = logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

// InitLoggerFile initializes the logger with config
func InitLoggerFile(logDir string, logToFile bool) error {
	if logToFile && logDir != "" {
		// Ensure log directory exists
		if err := os.MkdirAll(logDir, 0755); err != nil {
			return err
		}

		// Create or open log file
		logFile := filepath.Join(logDir, "tiny-lsm.log")
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return err
		}

		// Set output to file
		logger.SetOutput(file)
	}

	return nil
}

// SetLevel sets the logging level
func SetLevel(level logrus.Level) {
	logger.SetLevel(level)
}

// Debug logs a debug message
func Trace(args ...interface{}) {
	logger.Trace(args...)
}

// Tracef logs a trace message with formatting
func Tracef(format string, args ...interface{}) {
	logger.Tracef(format, args...)
}

// Debug logs a debug message
func Debug(args ...interface{}) {
	logger.Debug(args...)
}

// Debugf logs a debug message with formatting
func Debugf(format string, args ...interface{}) {
	logger.Debugf(format, args...)
}

// Info logs an info message
func Info(args ...interface{}) {
	logger.Info(args...)
}

// Infof logs an info message with formatting
func Infof(format string, args ...interface{}) {
	logger.Infof(format, args...)
}

// Warn logs a warning message
func Warn(args ...interface{}) {
	logger.Warn(args...)
}

// Warnf logs a warning message with formatting
func Warnf(format string, args ...interface{}) {
	logger.Warnf(format, args...)
}

// Error logs an error message
func Error(args ...interface{}) {
	logger.Error(args...)
}

// Errorf logs an error message with formatting
func Errorf(format string, args ...interface{}) {
	logger.Errorf(format, args...)
}

// Fatal logs a fatal message and exits
func Fatal(args ...interface{}) {
	logger.Fatal(args...)
}

// Fatalf logs a fatal message with formatting and exits
func Fatalf(format string, args ...interface{}) {
	logger.Fatalf(format, args...)
}
