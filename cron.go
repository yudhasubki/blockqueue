package blockqueue

import (
	"errors"
	"fmt"
	"strings"
	"time"

	cronlib "github.com/robfig/cron/v3"
)

type cronExpression struct{ schedule cronlib.Schedule }

var fiveFieldCronParser = cronlib.NewParser(
	cronlib.Minute | cronlib.Hour | cronlib.Dom | cronlib.Month | cronlib.Dow,
)

func parseCron(expression, timezone string) (cronExpression, error) {
	if len(strings.Fields(expression)) != 5 {
		return cronExpression{}, errors.New("cron must contain exactly five fields")
	}
	if timezone == "" {
		timezone = time.UTC.String()
	}
	if _, err := time.LoadLocation(timezone); err != nil {
		return cronExpression{}, fmt.Errorf("invalid IANA timezone %q: %w", timezone, err)
	}
	schedule, err := fiveFieldCronParser.Parse("CRON_TZ=" + timezone + " " + expression)
	if err != nil {
		return cronExpression{}, fmt.Errorf("invalid cron expression: %w", err)
	}
	return cronExpression{schedule: schedule}, nil
}

func (expression cronExpression) Next(after time.Time) time.Time {
	if expression.schedule == nil {
		return time.Time{}
	}
	return expression.schedule.Next(after).UTC()
}
