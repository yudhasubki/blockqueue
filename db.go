package blockqueue

import (
	"context"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/yudhasubki/blockqueue/pkg/core"
	"github.com/yudhasubki/blockqueue/pkg/sqlite"
)

var Conn *sqlite.SQLite

func getTopics(ctx context.Context, filter core.FilterTopic) (core.Topics, error) {
	var (
		topics = make(core.Topics, 0)
		query  = "SELECT * FROM topics"
	)

	clause, arg := filter.Filter("AND")
	if clause != "" {
		query += " WHERE " + clause
	}

	query, args, err := sqlx.Named(query, arg)
	if err != nil {
		return topics, err
	}

	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return topics, err
	}
	query = Conn.Database.Rebind(query)

	err = Conn.Database.Select(&topics, query, args...)
	if err != nil {
		return topics, err
	}

	return topics, nil
}

func getSubscribers(ctx context.Context, filter core.FilterSubscriber) (core.Subscribers, error) {
	var (
		subscribers = make(core.Subscribers, 0)
		query       = "SELECT topic_subscribers.*, t.name as topic_name FROM topic_subscribers INNER JOIN topics t ON topic_subscribers.topic_id = t.id"
	)

	clause, arg := filter.Filter("AND")
	if clause != "" {
		query += " WHERE " + clause
	}

	query, args, err := sqlx.Named(query, arg)
	if err != nil {
		return subscribers, err
	}

	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return subscribers, err
	}
	query = Conn.Database.Rebind(query)

	err = Conn.Database.Select(&subscribers, query, args...)
	if err != nil {
		return subscribers, err
	}

	return subscribers, nil
}

func createTxTopic(ctx context.Context, tx *sqlx.Tx, topic core.Topic) error {
	stmt, err := tx.PrepareNamedContext(ctx, "INSERT INTO topics (`id`, `name`) VALUES (:id, :name)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, topic)
	if err != nil {
		return err
	}

	return nil
}

func deleteTxTopic(ctx context.Context, tx *sqlx.Tx, topic core.Topic) error {
	stmt, err := tx.PrepareNamedContext(ctx, "UPDATE topics SET deleted_at = :deleted_at WHERE id = :id")
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, topic)
	if err != nil {
		return err
	}

	return nil
}

func deleteTxSubscribers(ctx context.Context, tx *sqlx.Tx, topic core.Subscriber) error {
	stmt, err := tx.PrepareNamedContext(ctx, "UPDATE topic_subscribers SET deleted_at = :deleted_at WHERE name = :name AND topic_id = :topic_id")
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, topic)
	if err != nil {
		return err
	}

	return nil
}

func createTxSubscribers(ctx context.Context, tx *sqlx.Tx, subscribers core.Subscribers) error {
	_, err := tx.NamedExecContext(ctx, "INSERT INTO topic_subscribers (`id`, `topic_id`, `name`, `option`) VALUES (:id, :topic_id, :name, :option)", subscribers)
	if err != nil {
		return err
	}

	return nil
}

func createMessages(ctx context.Context, message core.Message) error {
	stmt, err := Conn.Database.PrepareNamedContext(ctx, "INSERT INTO topic_messages (`id`, `topic_id`, `message`, `status`) VALUES (:id, :topic_id, :message, :status)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, message)
	if err != nil {
		return err
	}

	return nil
}

func updateStatusMessage(ctx context.Context, status core.MessageStatus, ids ...uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}

	query, args, err := sqlx.In("UPDATE topic_messages SET status = ? WHERE id IN (?)", status, ids)
	if err != nil {
		return err
	}

	_, err = Conn.Database.ExecContext(ctx, Conn.Database.Rebind(query), args...)
	if err != nil {
		return err
	}

	return nil
}

func getMessages(ctx context.Context, filter core.FilterMessage) (core.Messages, error) {
	var (
		messages = make(core.Messages, 0)
		query    = "SELECT * FROM topic_messages"
	)

	clause, arg := filter.Filter("AND")
	if clause != "" {
		query += " WHERE " + clause
	}
	query += " " + filter.Sort() + " " + filter.Page()

	query, args, err := sqlx.Named(query, arg)
	if err != nil {
		return messages, err
	}

	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return messages, err
	}
	query = Conn.Database.Rebind(query)

	err = Conn.Database.Select(&messages, query, args...)
	if err != nil {
		return messages, err
	}

	return messages, nil
}

func tx(ctx context.Context, fn func(ctx context.Context, tx *sqlx.Tx) error) error {
	tx, err := Conn.Database.Beginx()
	if err != nil {
		return err
	}

	err = fn(ctx, tx)
	if err != nil {
		if errTx := tx.Rollback(); errTx == nil {
			return err
		}

		return err
	}

	return tx.Commit()
}
