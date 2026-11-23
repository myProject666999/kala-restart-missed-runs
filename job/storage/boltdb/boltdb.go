package boltdb

import (
	"bytes"
	"encoding/gob"
	"os"
	"strings"
	"time"

	"github.com/ajvb/kala/job"

	log "github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
)

var (
	jobBucket = []byte("jobs")
)

func GetBoltDB(path string) *BoltJobDB {
	if path != "" && !strings.HasSuffix(path, "/") {
		path += "/"
	}
	path += "jobdb.db"
	var perms os.FileMode = 0o0600
	database, err := bolt.Open(path, perms, &bolt.Options{Timeout: time.Second * 10}) //nolint:gomnd
	if err != nil {
		log.Fatal(err)
	}
	return &BoltJobDB{
		path:   path,
		dbConn: database,
	}
}

type BoltJobDB struct {
	dbConn *bolt.DB
	path   string
}

func (db *BoltJobDB) Close() error {
	return db.dbConn.Close()
}

// GetAll returns all persisted Jobs.
//
// A single entry that cannot be decoded or parsed no longer aborts the whole
// read (v1 returned the error, and the caller called log.Fatal). Bad entries are
// skipped and summarized in a *job.JobsPartiallyLoadedError together with the
// jobs that did load. A failure to read the bucket itself is still returned as a
// whole-backend error.
func (db *BoltJobDB) GetAll() ([]*job.Job, error) {
	allJobs := []*job.Job{}
	failures := make([]job.JobLoadFailure, 0)

	err := db.dbConn.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(jobBucket)
		if err != nil {
			return err
		}

		return bucket.ForEach(func(k, v []byte) error {
			j, err := decodeJob(v)
			if err != nil {
				failures = append(failures, job.JobLoadFailure{Id: string(k), Err: err})
				return nil
			}
			allJobs = append(allJobs, j)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	if len(failures) > 0 {
		return allJobs, &job.JobsPartiallyLoadedError{Jobs: allJobs, Failures: failures}
	}

	return allJobs, nil
}

// decodeJob decodes one stored job and initializes its parsed schedule fields.
func decodeJob(v []byte) (*job.Job, error) {
	buffer := bytes.NewBuffer(v)
	dec := gob.NewDecoder(buffer)
	j := new(job.Job)
	if err := dec.Decode(j); err != nil {
		return nil, err
	}
	if err := j.InitDelayDuration(false); err != nil {
		return nil, err
	}
	return j, nil
}

func (db *BoltJobDB) Get(id string) (*job.Job, error) {
	j := new(job.Job)

	err := db.dbConn.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(jobBucket)

		v := b.Get([]byte(id))
		if v == nil {
			return job.ErrJobNotFound(id)
		}

		buf := bytes.NewBuffer(v)
		err := gob.NewDecoder(buf).Decode(j)

		return err
	})
	if err != nil {
		return nil, err
	}

	j.Id = id
	return j, nil
}

func (db *BoltJobDB) Delete(id string) error {
	err := db.dbConn.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(jobBucket)
		return bucket.Delete([]byte(id))
	})
	return err
}

func (db *BoltJobDB) Save(j *job.Job) error {
	err := db.dbConn.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(jobBucket)
		if err != nil {
			return err
		}

		buffer := new(bytes.Buffer)
		enc := gob.NewEncoder(buffer)
		err = enc.Encode(j)
		if err != nil {
			return err
		}

		err = bucket.Put([]byte(j.Id), buffer.Bytes())
		if err != nil {
			return err
		}
		return nil
	})
	return err
}
