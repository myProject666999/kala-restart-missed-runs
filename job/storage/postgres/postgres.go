package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/lib/pq"

	"github.com/ajvb/kala/job"

	log "github.com/sirupsen/logrus"
)

const TABLE_NAME = "jobs"

type DB struct {
	conn *sql.DB
}

// New instantiates a new DB.
func New(dsn string) *DB {
	connection, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}
	// passive attempt to create table
	_, _ = connection.Exec(fmt.Sprintf(`create table %s (job jsonb);`, TABLE_NAME))
	return &DB{
		conn: connection,
	}
}

// GetAll returns all persisted Jobs.
//
// One bad row no longer aborts the whole read; it is summarized in a
// *job.JobsPartiallyLoadedError together with the jobs that did load.
func (d DB) GetAll() ([]*job.Job, error) {
	query := fmt.Sprintf(`select coalesce(json_agg(j.job), '[]'::json) from (select * from %[1]s) as j;`, TABLE_NAME)

	var r sql.NullString
	err := d.conn.QueryRow(query).Scan(&r)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	jobs := []*job.Job{}
	if r.Valid {
		if err := json.Unmarshal([]byte(r.String), &jobs); err != nil {
			return nil, err
		}
	}

	jobsInitiated := []*job.Job{}
	failures := make([]job.JobLoadFailure, 0)
	for _, j := range jobs {
		if err := j.InitDelayDuration(false); err != nil {
			id := ""
			if j != nil {
				id = j.Id
			}
			failures = append(failures, job.JobLoadFailure{Id: id, Err: err})
			continue
		}
		jobsInitiated = append(jobsInitiated, j)
	}

	if len(failures) > 0 {
		return jobsInitiated, &job.JobsPartiallyLoadedError{Jobs: jobsInitiated, Failures: failures}
	}
	return jobsInitiated, nil
}

// Get returns a persisted Job.
func (d DB) Get(id string) (*job.Job, error) {
	template := `select to_jsonb(j.job) from (select * from %[1]s where job ->> 'id' = $1) as j;`
	query := fmt.Sprintf(template, TABLE_NAME)
	var r sql.NullString
	err := d.conn.QueryRow(query, id).Scan(&r)
	if err != nil {
		return nil, err
	}
	result := &job.Job{}
	if r.Valid {
		err = json.Unmarshal([]byte(r.String), &result)
	}
	return result, err
}

// Delete deletes a persisted Job.
func (d DB) Delete(id string) error {
	query := fmt.Sprintf(`delete from %v where job ->> 'id' = $1;`, TABLE_NAME)
	_, err := d.conn.Exec(query, id)
	return err
}

// Save persists a Job.
func (d DB) Save(j *job.Job) error {
	template := `update %[1]s SET job = $1 where job ->> 'id' = '` + j.Id + `'`

	_, errFind := d.Get(j.Id)
	if errFind != nil {
		template = `insert into %[1]s (job) values($1);`
	}

	query := fmt.Sprintf(template, TABLE_NAME)
	r, err := json.Marshal(j)
	if err != nil {
		return err
	}
	transaction, err := d.conn.Begin()
	if err != nil {
		return err
	}
	statement, err := transaction.Prepare(query)
	if err != nil {
		transaction.Rollback() //nolint:errcheck // adding insult to injury
		return err
	}
	defer statement.Close()
	_, err = statement.Exec(string(r))
	if err != nil {
		transaction.Rollback() //nolint:errcheck // adding insult to injury
		return err
	}
	return transaction.Commit()
}

// Close closes the connection to Postgres.
func (d DB) Close() error {
	return d.conn.Close()
}
