package redismq

import (
	"fmt"
	"strconv"
	"time"

	"gopkg.in/redis.v2"
)

// q is the central element of this library.
// Packages can be put into or get from the q.
// To read from a q you need a consumer.
type Queue struct {
	redisClient    *redis.Client
	Name           string
	rateStatsCache map[int64]map[string]int64
	rateStatsChan  chan (*dataPoint)
	lastStatsWrite int64
}

type dataPoint struct {
	name  string
	value int64
	incr  bool
}

// CreateQueue return a q that you can Put() or AddConsumer() to
// Works like SelectQueue for existing queues
func CreateQueue(redisHost, redisPort, redisPassword string, redisDB int64, name string) *Queue {
	return newQueue(redisHost, redisPort, redisPassword, redisDB, name)
}

// SelectQueue returns a q if a q with the name exists
func SelectQueue(redisHost, redisPort, redisPassword string, redisDB int64, name string) (q *Queue, err error) {
	redisClient := redis.NewTCPClient(&redis.Options{
		Addr:     redisHost + ":" + redisPort,
		Password: redisPassword,
		DB:       redisDB,
	})
	defer redisClient.Close()

	isMember, err := redisClient.SIsMember(masterQueueKey(), name).Result()
	if err != nil {
		return nil, err
	}
	if !isMember {
		return nil, fmt.Errorf("q with this name doesn't exist")
	}

	return newQueue(redisHost, redisPort, redisPassword, redisDB, name), nil
}

func newQueue(redisHost, redisPort, redisPassword string, redisDB int64, name string) *Queue {
	q := &Queue{Name: name}
	q.redisClient = redis.NewTCPClient(&redis.Options{
		Addr:     redisHost + ":" + redisPort,
		Password: redisPassword,
		DB:       redisDB,
	})
	q.redisClient.SAdd(masterQueueKey(), name)
	q.startStatsWriter()
	return q
}

// Delete clears all input and failed queues as well as all consumers
// will not proceed as long as consumers are running
func (q *Queue) Delete() error {
	consumers, err := q.getConsumers()
	if err != nil {
		return err
	}

	for _, name := range consumers {
		if q.isActiveConsumer(name) {
			return fmt.Errorf("cannot delete q with active consumers")
		}

		consumer, err := q.AddConsumer(name)
		if err != nil {
			return err
		}

		err = consumer.ResetWorking()
		if err != nil {
			return err
		}

		err = q.redisClient.SRem(queueWorkersKey(q.Name), name).Err()
		if err != nil {
			return err
		}
	}

	err = q.ResetInput()
	if err != nil {
		return err
	}

	err = q.ResetFailed()
	if err != nil {
		return err
	}

	err = q.redisClient.SRem(masterQueueKey(), q.Name).Err()
	if err != nil {
		return err
	}
	err = q.redisClient.Del(queueWorkersKey(q.Name)).Err()
	if err != nil {
		return err
	}

	q.redisClient.Close()

	return nil
}

// Put writes the payload into the input q
func (q *Queue) Put(payload []byte) error {
	p := &Package{CreatedAt: time.Now(), Payload: payload, Queue: q}
	lpush := q.redisClient.LPush(queueInputKey(q.Name), p.getString())
	q.incrRate(queueInputRateKey(q.Name), 1)
	return lpush.Err()
}

// PutString writes the payload as astring into the input q
func (q *Queue) PutString(payload string) error {
	p := &Package{CreatedAt: time.Now(), Payload: []byte(payload), Queue: q}
	lpush := q.redisClient.LPush(queueInputKey(q.Name), p.getString())
	q.incrRate(queueInputRateKey(q.Name), 1)
	return lpush.Err()
}

// RequeueFailed moves all failed packages back to the input q
func (q *Queue) RequeueFailed() error {
	l := q.GetFailedLength()
	// TODO implement this in lua
	for l > 0 {
		err := q.redisClient.RPopLPush(queueFailedKey(q.Name), queueInputKey(q.Name)).Err()
		if err != nil {
			return err
		}
		q.incrRate(queueInputRateKey(q.Name), 1)
		l--
	}
	return nil
}

// ResetInput deletes all packages from the input q
func (q *Queue) ResetInput() error {
	return q.redisClient.Del(queueInputKey(q.Name)).Err()
}

// ResetFailed deletes all packages from the failed q
func (q *Queue) ResetFailed() error {
	return q.redisClient.Del(queueFailedKey(q.Name)).Err()
}

// GetInputLength returns the number of packages in the input q
func (q *Queue) GetInputLength() int64 {
	return q.redisClient.LLen(queueInputKey(q.Name)).Val()
}

// GetFailedLength returns the number of packages in the failed q
func (q *Queue) GetFailedLength() int64 {
	return q.redisClient.LLen(queueFailedKey(q.Name)).Val()
}

func (q *Queue) getConsumers() (consumers []string, err error) {
	return q.redisClient.SMembers(queueWorkersKey(q.Name)).Result()
}

func (q *Queue) incrRate(name string, value int64) {
	dp := &dataPoint{name: name, value: value}
	q.rateStatsChan <- dp
}

func (q *Queue) startStatsWriter() {
	q.rateStatsCache = make(map[int64]map[string]int64)
	q.rateStatsChan = make(chan *dataPoint, 2E6)
	writing := false
	go func() {
		for dp := range q.rateStatsChan {
			now := time.Now().UTC().Unix()
			if q.rateStatsCache[now] == nil {
				q.rateStatsCache[now] = make(map[string]int64)
			}
			q.rateStatsCache[now][dp.name] += dp.value
			if now > q.lastStatsWrite && !writing {
				writing = true
				q.writeStatsCacheToRedis(now)
				writing = false
			}
		}
	}()
	return
}

func (q *Queue) writeStatsCacheToRedis(now int64) {
	for sec := range q.rateStatsCache {
		if sec >= now-1 {
			continue
		}

		for name, value := range q.rateStatsCache[sec] {
			key := fmt.Sprintf("%s::%d", name, sec)
			// incrby can handle the situation where multiple inputs are counted
			q.redisClient.IncrBy(key, value)
			// save stats to redis with 2h expiration
			q.redisClient.Expire(key, 2*time.Hour)
		}
		// track q lengths
		inputKey := fmt.Sprintf("%s::%d", queueInputSizeKey(q.Name), now)
		failKey := fmt.Sprintf("%s::%d", queueFailedSizeKey(q.Name), now)
		q.redisClient.SetEx(inputKey, 2*time.Hour, strconv.FormatInt(q.GetInputLength(), 10))
		q.redisClient.SetEx(failKey, 2*time.Hour, strconv.FormatInt(q.GetFailedLength(), 10))

		delete(q.rateStatsCache, sec)
	}
	q.lastStatsWrite = now
}

func (q *Queue) isActiveConsumer(name string) bool {
	val := q.redisClient.Get(consumerHeartbeatKey(q.Name, name)).Val()
	return val == "ping"
}
