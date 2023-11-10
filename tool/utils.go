package tool

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
)

type RuntimeError struct {
	Stage string
	Err   error
}

func (re *RuntimeError) Error() string {
	return re.Stage + ": " + re.Err.Error()
}

func Check(addresses []string) error {
	if len(addresses) == 0 {
		return errors.New("addresses is empty")
	}
	m := make(map[string]byte)
	for _, address := range addresses {
		if len(address) == 0 {
			return errors.New("empty ip address")
		}
		sp := strings.Split(address, ":")
		if len(sp) == 1 {
			return errors.New(address + "'s port is not specified")
		}
		port, err := strconv.Atoi(sp[1])
		if err != nil || port <= 0 {
			return errors.New(address + "'s port:" + sp[1] + " is invalid")
		}
		ipAddr := sp[0]
		if "localhost" != ipAddr && !valid(ipAddr) {
			return errors.New(ipAddr + " is not a valid ip address")
		}
		if _, duplicate := m[address]; duplicate {
			return errors.New(address + " is duplicate")
		}
		m[address] = 1
	}
	return nil
}

func valid(queryIP string) bool {
	if sp := strings.Split(queryIP, "."); len(sp) == 4 {
		for _, s := range sp {
			if len(s) > 1 && s[0] == '0' {
				return false
			}
			if v, err := strconv.Atoi(s); err != nil || v > 255 {
				return false
			}
		}
		return true
	}
	if sp := strings.Split(queryIP, ":"); len(sp) == 8 {
		for _, s := range sp {
			if len(s) > 4 {
				return false
			}
			if _, err := strconv.ParseUint(s, 16, 64); err != nil {
				return false
			}
		}
		return true
	}
	return false
}

func ChooseServer(lastLeader int, serverCount int) int {
	if lastLeader >= 0 && lastLeader < serverCount {
		return lastLeader
	} else {
		x := rand.Intn(serverCount)
		return x
	}
}

// MaxInt 返回两个整数中的最大值
func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// MinInt 返回两个整数中的最小值
func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// IsPrime 判断一个整数是否是素数
func IsPrime(n int) bool {
	if n <= 1 {
		return false
	}
	maxDivisor := int(math.Sqrt(float64(n)))
	for i := 2; i <= maxDivisor; i++ {
		if n%i == 0 {
			return false
		}
	}
	return true
}

// Greet 打印欢迎消息
func Greet(name string) {
	fmt.Printf("Hello, %s!\n", name)
}
