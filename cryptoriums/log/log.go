package log

import (
	"os"
	"time"

	"cosmossdk.io/log"
	"github.com/rs/zerolog"
)

func New() log.Logger {
	zerolog.TimeFieldFormat = time.StampMilli

	return log.NewLogger(
		os.Stderr,
		log.LevelOption(zerolog.DebugLevel),
		log.ColorOption(false),
		log.TimeFormatOption(time.StampMilli),
	)
}
