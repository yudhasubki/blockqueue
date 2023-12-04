package core

import (
	"strings"

	"github.com/google/uuid"
	"gopkg.in/guregu/null.v4"
)

type Topic struct {
	Id        uuid.UUID   `db:"id"`
	Name      string      `db:"name"`
	CreatedAt string      `db:"created_at"`
	DeletedAt null.String `db:"deleted_at"`
}

func (t Topic) Empty() bool {
	return t == Topic{}
}

type Topics []Topic

func (topics Topics) Ids() []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(topics))
	for _, topic := range topics {
		ids = append(ids, topic.Id)
	}

	return ids
}

type FilterTopic struct {
	Name        []string
	WithDeleted bool
}

func (f FilterTopic) Filter(operator string) (string, map[string]interface{}) {
	var (
		clause []string
		args   = make(map[string]interface{})
	)

	if len(f.Name) > 0 {
		args["name"] = f.Name
		clause = append(clause, "name IN(:name)")
	}

	if f.WithDeleted {
		clause = append(clause, "deleted_at IS NOT NULL")
	} else {
		clause = append(clause, "deleted_at IS NULL")
	}

	return strings.Join(clause, " "+operator+" "), args
}
