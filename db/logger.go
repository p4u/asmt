package db

import (
	"github.com/aergoio/aergo-lib/log"
)

type extendedLog struct {
	*log.Logger
}

func (l *extendedLog) Errorf(f string, v ...interface{}) {
	l.Error().Msgf(f, v...)
}

func (l *extendedLog) Warningf(f string, v ...interface{}) {
	l.Warn().Msgf(f, v...)
}

func (l *extendedLog) Infof(f string, v ...interface{}) {
	// reduce info to debug level because infos at badgerdb are too detail
	l.Debug().Msgf(f, v...) // INFO -> DEBUG
}

func (l *extendedLog) Debugf(f string, v ...interface{}) {
	l.Debug().Msgf(f, v...)
}
