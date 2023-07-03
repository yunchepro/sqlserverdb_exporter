package logutil

import (
	"io/ioutil"
	"time"

	"github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	log "github.com/sirupsen/logrus"
)

var (
	logLevels = map[string]log.Level{
		"debug": log.DebugLevel,
		"info":  log.InfoLevel,
		"warn":  log.WarnLevel,
		"error": log.ErrorLevel}
)

func InitLog(logfile string, loglevel string) {
	hook := newLfsHook(logfile, loglevel, 3)
	log.AddHook(hook)
	log.SetOutput(ioutil.Discard)
}

func GetLogger(filename string, level string) *log.Logger {
	logger := log.New()
	hook := newLfsHook(filename, level, 3)
	logger.AddHook(hook)
	return logger
}

func newLfsHook(logName string, logLevel string, maxRemainCnt uint) log.Hook {
	writer, err := rotatelogs.New(
		logName+".%Y%m%d%H",
		// WithLinkName为最新的日志建立软连接，以方便随着找到当前日志文件
		rotatelogs.WithLinkName(logName),

		// WithRotationTime设置日志分割的时间，这里设置为一小时分割一次
		rotatelogs.WithRotationTime(time.Hour),

		// WithMaxAge和WithRotationCount二者只能设置一个，
		// WithMaxAge设置文件清理前的最长保存时间，
		// WithRotationCount设置文件清理前最多保存的个数。
		//rotatelogs.WithMaxAge(time.Hour*24),
		rotatelogs.WithRotationCount(maxRemainCnt),
	)

	if err != nil {
		log.Errorf("config local file system for logger error: %v", err)
	}

	level, ok := logLevels[logLevel]

	if ok {
		log.SetLevel(level)
	} else {
		log.SetLevel(log.WarnLevel)
	}

	lfsHook := lfshook.NewHook(lfshook.WriterMap{
		log.DebugLevel: writer,
		log.InfoLevel:  writer,
		log.WarnLevel:  writer,
		log.ErrorLevel: writer,
		log.FatalLevel: writer,
		log.PanicLevel: writer,
	}, &log.TextFormatter{DisableColors: true})

	return lfsHook
}
