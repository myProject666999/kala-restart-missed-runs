package boltdb

import (
	"errors"
	"testing"
	"time"

	"github.com/ajvb/kala/job"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

var testDbPath = ""

func setupTest(t *testing.T) {
	db := GetBoltDB(testDbPath)
	defer db.Close()

	jobs, err := db.GetAll()
	assert.NoError(t, err)

	for _, j := range jobs {
		err = db.Delete(j.Id)
		assert.NoError(t, err)
	}

	// Remove any corrupt entries a test may have written, so they do not
	// leak into later tests sharing the same database file.
	if err := db.dbConn.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(jobBucket)
		if bucket == nil {
			return nil
		}
		var keys [][]byte
		if err := bucket.ForEach(func(k, _ []byte) error {
			kk := make([]byte, len(k))
			copy(kk, k)
			keys = append(keys, kk)
			return nil
		}); err != nil {
			return err
		}
		for _, k := range keys {
			if err := bucket.Delete(k); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSaveAndGetJob(t *testing.T) {
	setupTest(t)

	db := GetBoltDB(testDbPath)
	cache := job.NewLockFreeJobCache(db)
	defer db.Close()

	genericMockJob := job.GetMockJobWithGenericSchedule(time.Now())
	genericMockJob.Init(cache)
	db.Save(genericMockJob)

	j, err := db.Get(genericMockJob.Id)
	assert.Nil(t, err)

	assert.WithinDuration(t, j.NextRunAt, genericMockJob.NextRunAt, 100*time.Microsecond)
	assert.Equal(t, j.Name, genericMockJob.Name)
	assert.Equal(t, j.Id, genericMockJob.Id)
	assert.Equal(t, j.Command, genericMockJob.Command)
	assert.Equal(t, j.Schedule, genericMockJob.Schedule)
	assert.Equal(t, j.Owner, genericMockJob.Owner)
	assert.Equal(t, j.Metadata.SuccessCount, genericMockJob.Metadata.SuccessCount)
}

func TestDeleteJob(t *testing.T) {
	setupTest(t)

	db := GetBoltDB(testDbPath)
	cache := job.NewLockFreeJobCache(db)
	defer db.Close()

	genericMockJob := job.GetMockJobWithGenericSchedule(time.Now())
	genericMockJob.Init(cache)
	db.Save(genericMockJob)

	// Make sure its there
	j, err := db.Get(genericMockJob.Id)
	assert.Nil(t, err)
	assert.Equal(t, j.Name, genericMockJob.Name)
	retrievedJob, err := cache.Get(genericMockJob.Id)
	assert.NoError(t, err)
	assert.NotNil(t, retrievedJob)

	// Delete it
	genericMockJob.Delete(cache)

	k, err := db.Get(genericMockJob.Id)
	assert.Error(t, err)
	assert.Nil(t, k)
	retrievedJobTwo, err := cache.Get(genericMockJob.Id)
	assert.Error(t, err)
	assert.Nil(t, retrievedJobTwo)

	genericMockJob.Delete(cache)
}

func TestSaveAndGetAllJobs(t *testing.T) {
	setupTest(t)

	db := GetBoltDB(testDbPath)
	cache := job.NewLockFreeJobCache(db)
	defer db.Close()

	genericMockJobOne := job.GetMockJobWithGenericSchedule(time.Now())
	genericMockJobOne.Init(cache)
	err := db.Save(genericMockJobOne)
	assert.NoError(t, err)

	genericMockJobTwo := job.GetMockJobWithGenericSchedule(time.Now())
	genericMockJobTwo.Init(cache)
	err = db.Save(genericMockJobTwo)
	assert.NoError(t, err)

	jobs, err := db.GetAll()
	assert.Nil(t, err)
	assert.Equal(t, len(jobs), 2)
}

// TestGetAllIsolatesBadEntry verifies that one undecodable stored entry does
// not abort the whole read: the good jobs load, the bad one is summarized in a
// *job.JobsPartiallyLoadedError, and GetAll does not claim zero failures.
func TestGetAllIsolatesBadEntry(t *testing.T) {
	setupTest(t)

	db := GetBoltDB(testDbPath)
	defer db.Close()

	good := job.GetMockJobWithGenericSchedule(time.Now())
	good.Id = "good-entry"
	// The job does not need a live cache just to be saved.
	if err := db.Save(good); err != nil {
		t.Fatal(err)
	}

	// Write a deliberately corrupt record directly into the bucket.
	if err := db.dbConn.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(jobBucket)
		if err != nil {
			return err
		}
		return bucket.Put([]byte("bad-entry"), []byte("this is not gob"))
	}); err != nil {
		t.Fatal(err)
	}

	jobs, err := db.GetAll()
	var partial *job.JobsPartiallyLoadedError
	if !errors.As(err, &partial) {
		t.Fatalf("expected a partial-load error, got %v", err)
	}
	assert.Equal(t, 1, len(jobs), "the good job still loads")
	require.Len(t, partial.Failures, 1)
	assert.Equal(t, "bad-entry", partial.Failures[0].Id)
}
