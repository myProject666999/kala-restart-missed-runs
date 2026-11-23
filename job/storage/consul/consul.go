package consul

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/ajvb/kala/job"

	"github.com/hashicorp/consul/api"

	log "github.com/sirupsen/logrus"
)

var (
	prefix = "kala/jobs/"
)

func New(address string) *ConsulJobDB {
	config := api.DefaultConfig()
	if address != "" {
		config.Address = address
	}
	client, err := api.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}
	return &ConsulJobDB{
		conn: client.KV(),
	}
}

type ConsulJobDB struct {
	conn *api.KV
}

func (db *ConsulJobDB) Close() error {
	return nil
}

// GetAll returns all persisted Jobs.
//
// A bad entry is skipped and summarized in a *job.JobsPartiallyLoadedError
// instead of being silently dropped (v1 did that via `continue`, so failures
// were invisible) or aborting the read.
func (db *ConsulJobDB) GetAll() ([]*job.Job, error) {
	allJobs := []*job.Job{}
	failures := make([]job.JobLoadFailure, 0)

	pairs, _, err := db.conn.List(prefix, &api.QueryOptions{RequireConsistent: true})
	if err != nil {
		return allJobs, err
	}
	for _, pair := range pairs {
		buffer := bytes.NewBuffer(pair.Value)
		dec := json.NewDecoder(buffer)
		j := new(job.Job)
		if err := dec.Decode(j); err != nil {
			failures = append(failures, job.JobLoadFailure{Id: pair.Key, Err: err})
			continue
		}
		if err := j.InitDelayDuration(false); err != nil {
			failures = append(failures, job.JobLoadFailure{Id: j.Id, Err: err})
			continue
		}
		allJobs = append(allJobs, j)
	}

	if len(failures) > 0 {
		return allJobs, &job.JobsPartiallyLoadedError{Jobs: allJobs, Failures: failures}
	}
	return allJobs, nil
}

func (db *ConsulJobDB) Get(id string) (*job.Job, error) {
	j := new(job.Job)

	pair, _, err := db.conn.Get(prefix+id, &api.QueryOptions{RequireConsistent: true})
	if err != nil {
		return nil, err
	}
	if pair == nil {
		return nil, fmt.Errorf("ID %s not found", id)
	}
	buf := bytes.NewBuffer(pair.Value)
	err = json.NewDecoder(buf).Decode(j)
	if err != nil {
		return nil, err
	}
	return j, nil
}

func (db *ConsulJobDB) Delete(id string) error {
	_, err := db.conn.Delete(prefix+id, &api.WriteOptions{})
	return err
}

func (db *ConsulJobDB) Save(j *job.Job) error {
	buffer := new(bytes.Buffer)
	enc := json.NewEncoder(buffer)
	err := enc.Encode(j)
	if err != nil {
		return err
	}
	pair := &api.KVPair{Key: prefix + j.Id, Value: buffer.Bytes()}
	_, err = db.conn.Put(pair, &api.WriteOptions{})
	return err
}
