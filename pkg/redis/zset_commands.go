package redis

import (
	"fmt"
	"strings"
)

// ======================== ZSet Commands ========================

// ZSetElement represents an element in a sorted set
type ZSetElement struct {
	Member string
	Score  float64
}

// expireCleanZSet checks and cleans expired sorted set data
func (r *RedisWrapper) expireCleanZSet(key string) bool {
	// TODO: Lab 6.4

	return false
}

// extractScoreFromKey extracts score from a SCORE_ key
func (r *RedisWrapper) extractScoreFromKey(scoreKey string) string {
	scorePrefix := "_SCORE_"
	pos := strings.Index(scoreKey, scorePrefix)
	if pos == -1 {
		return ""
	}
	return scoreKey[pos+len(scorePrefix):]
}

// ZAdd implements Redis ZADD command
func (r *RedisWrapper) ZAdd(args []string) string {
	// TODO: Lab 6.4

	addedCount := 0
	return fmt.Sprintf(":%d\r\n", addedCount)
}

// ZRem implements Redis ZREM command
func (r *RedisWrapper) ZRem(args []string) string {
	// TODO: Lab 6.4

	removedCount := 0
	return fmt.Sprintf(":%d\r\n", removedCount)
}

// ZRange implements Redis ZRANGE command
func (r *RedisWrapper) ZRange(args []string) string {
	// TODO: Lab 6.4

	result := ""
	return result
}

// ZCard implements Redis ZCARD command
func (r *RedisWrapper) ZCard(args []string) string {
	// TODO: Lab 6.4

	return ":0\r\n"
}

// ZScore implements Redis ZSCORE command
func (r *RedisWrapper) ZScore(args []string) string {
	// TODO: Lab 6.4

	return "$len(scoreValue)\r\n%scoreValue\r\n"
}

// ZIncrBy implements Redis ZINCRBY command
func (r *RedisWrapper) ZIncrBy(args []string) string {
	// TODO: Lab 6.4

	return ":%newScoreStr\r\n"
}

// ZRank implements Redis ZRANK command
func (r *RedisWrapper) ZRank(args []string) string {
	// TODO: Lab 6.4

	return "$rank_num\r\n"
}
