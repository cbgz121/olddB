package cmd

import (
	"fmt"
	"oldrosedb"
	"oldrosedb/utils"
	"strconv"
	"strings"

	"github.com/tidwall/redcon"
)

func zAdd(db *oldrosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("zadd")
		return
	}
	score, err := utils.StrToFloat64(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}
	if err = db.ZAdd([]byte(args[0]), score, []byte(args[2])); err == nil {
		res = okResult
	}
	return
}

func zScore(db *oldrosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("zscore")
		return
	}
	ok, score := db.ZScore([]byte(args[0]), []byte(args[1]))
	if ok {
		res = utils.Float64ToStr(score)
	}
	return
}

func zCard(db *oldrosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("zcard")
		return
	}
	card := db.ZCard([]byte(args[0]))
	res = redcon.SimpleInt(card)
	return
}

func zRank(db *oldrosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = ErrSyntaxIncorrect
		return
	}
	rank := db.ZRank([]byte(args[0]), []byte(args[1]))
	res = redcon.SimpleInt(rank)
	return
}

func zRevRank(db *oldrosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("zrevrank")
		return
	}
	rank := db.ZRevRank([]byte(args[0]), []byte(args[1]))
	res = redcon.SimpleInt(rank)
	return
}

func zIncrBy(db *oldrosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("zincrby")
		return
	}
	incr, err := utils.StrToFloat64(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}
	var val float64
	if val, err = db.ZIncrBy([]byte(args[0]), incr, []byte(args[2])); err == nil {
		res = utils.Float64ToStr(val)
	}
	return
}

func zRange(db *oldrosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 && len(args) != 4 {
		err = newWrongNumOfArgsError("zrange")
		return
	}
	return zRawRange(db, args, false)
}

func zRevRange(db *oldrosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 && len(args) != 4 {
		err = newWrongNumOfArgsError("zrevrange")
		return
	}
	return zRawRange(db, args, true)
}

// for zRange and zRevRange
func zRawRange(db *oldrosedb.RoseDB, args []string, rev bool) (res interface{}, err error) {
	withScores := false
	if len(args) == 4 {
		if strings.ToLower(args[3]) == "withscores" {
			withScores = true
			args = args[:3]
		} else {
			err = ErrSyntaxIncorrect
			return
		}
	}
	start, err := strconv.Atoi(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}
	end, err := strconv.Atoi(args[2])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}

	var val []interface{}
	if rev {
		if withScores {
			val = db.ZRevRangeWithScores([]byte(args[0]), start, end)
		} else {
			val = db.ZRevRange([]byte(args[0]), start, end)
		}
	} else {
		if withScores {
			val = db.ZRangeWithScores([]byte(args[0]), start, end)
		} else {
			val = db.ZRange([]byte(args[0]), start, end)
		}
	}

	results := make([]string, len(val))
	for i, v := range val {
		results[i] = fmt.Sprintf("%v", v)
	}
	res = results
	return
}

func zRem(db *oldrosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = ErrSyntaxIncorrect
		return
	}
	var ok bool
	if ok, err = db.ZRem([]byte(args[0]), []byte(args[1])); err == nil {
		if ok {
			res = redcon.SimpleInt(1)
		} else {
			res = redcon.SimpleInt(0)
		}
	}
	return
}

func zGetByRank(db *oldrosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("zgetbyrank")
		return
	}
	return zRawGetByRank(db, args, false)
}

func zRevGetByRank(db *oldrosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("zrevgetbyrank")
		return
	}
	return zRawGetByRank(db, args, true)
}

// for zGetByRank and zRevGetByRank
func zRawGetByRank(db *oldrosedb.RoseDB, args []string, rev bool) (res interface{}, err error) {
	rank, err := strconv.Atoi(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}

	var val []interface{}
	if rev {
		val = db.ZRevGetByRank([]byte(args[0]), rank)
	} else {
		val = db.ZGetByRank([]byte(args[0]), rank)
	}
	results := make([]string, len(val))
	for i, v := range val {
		results[i] = fmt.Sprintf("%v", v)
	}
	res = results
	return
}

func zScoreRange(db *oldrosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("zscorerange")
		return
	}
	return zRawScoreRange(db, args, false)
}

func zSRevScoreRange(db *oldrosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("zsrevscorerange")
		return
	}
	return zRawScoreRange(db, args, true)
}

// for zScoreRange and zSRevScoreRange
func zRawScoreRange(db *oldrosedb.RoseDB, args []string, rev bool) (res interface{}, err error) {
	param1, err := utils.StrToFloat64(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}
	param2, err := utils.StrToFloat64(args[2])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}
	var val []interface{}
	if rev {
		val = db.ZRevScoreRange([]byte(args[0]), param1, param2)
	} else {
		val = db.ZScoreRange([]byte(args[0]), param1, param2)
	}
	results := make([]string, len(val))
	for i, v := range val {
		results[i] = fmt.Sprintf("%v", v)
	}
	res = results
	return
}

func zKeyExists(db *oldrosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("zvalexists")
		return
	}

	if ok := db.ZKeyExists([]byte(args[0])); ok {
		res = redcon.SimpleInt(1)
	} else {
		res = redcon.SimpleInt(0)
	}
	return
}

func zClear(db *oldrosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("zclear")
		return
	}

	if err = db.ZClear([]byte(args[0])); err == nil {
		res = redcon.SimpleInt(1)
	} else {
		res = redcon.SimpleInt(0)
	}

	return
}

func zExpire(db *oldrosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("zexpire")
		return
	}

	var dur int64
	dur, err = strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}

	if err = db.ZExpire([]byte(args[0]), dur); err == nil {
		res = redcon.SimpleInt(1)
	} else {
		res = redcon.SimpleInt(0)
	}

	return
}

func zTTL(db *oldrosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("zttl")
		return
	}

	var ttl int64
	ttl = db.ZTTL([]byte(args[0]))

	res = redcon.SimpleInt(ttl)

	return
}

func init() {
	addExecCommand("zadd", zAdd)
	addExecCommand("zscore", zScore)
	addExecCommand("zcard", zCard)
	addExecCommand("zrank", zRank)
	addExecCommand("zrevrank", zRevRank)
	addExecCommand("zincrby", zIncrBy)
	addExecCommand("zrange", zRange)
	addExecCommand("zrevrange", zRevRange)
	addExecCommand("zrem", zRem)
	addExecCommand("zgetbyrank", zGetByRank)
	addExecCommand("zrevgetbyrank", zRevGetByRank)
	addExecCommand("zscorerange", zScoreRange)
	addExecCommand("zrevscorerange", zSRevScoreRange)
	addExecCommand("zkeyexists", zKeyExists)
	addExecCommand("zclear", zClear)
	addExecCommand("zexpire", zExpire)
	addExecCommand("zttl", zTTL)
}
