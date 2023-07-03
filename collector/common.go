package collector

import (
	"crypto/md5"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"io/ioutil"

	"gopkg.in/yaml.v2"
)

var (
	labelRemovePattern = regexp.MustCompile(`[:()*/%<>=&-]`)
	labelRemoveDup     = regexp.MustCompile("  +")
)

func formatInList(params []string) string {
	var s []string
	for _, p := range params {
		s = append(s, "'"+p+"'")
	}
	return strings.Join(s, ",")
}

// replace space with _, remove invalid char,
func formatLabel(s string) string {

	ns := labelRemoveDup.ReplaceAllString(labelRemovePattern.ReplaceAllString(s, ""), "_")
	vs := strings.Replace(strings.ToLower(ns), " ", "_", -1)
	return strings.TrimSuffix(vs, "_")
}

func formatFloat64(val float64) string {
	return strconv.FormatFloat(val, 'f', 0, 64)
}

func formatInt64(val int64) string {
	return strconv.FormatInt(val, 10)
}

func loadContext() (map[string]string, error) {
	var c = make(map[string]string)
	buf, err := ioutil.ReadFile("context.yaml")
	if err != nil {
		// return empty map
		return c, err
	}

	err = yaml.Unmarshal(buf, &c)
	return c, err
}

func saveContext(c map[string]string) error {
	out, err := yaml.Marshal(c)
	ioutil.WriteFile("context.yaml", out, 0666)
	return err
}

func parseVersion(vs string) (float64, error) {
	elems := strings.Split(vs, ".")
	prefix := len(elems)
	if prefix > 2 {
		prefix = 2
	}
	vv := strings.Join(elems[0:prefix], ".")
	return strconv.ParseFloat(vv, 64)
}

func tailOf(s string, sep string) string {
	parts := strings.Split(s, sep)
	return parts[len(parts)-1]
}

func formatTime(t time.Time) string {
	return t.Format("2006-01-02 15:04:05")
}

func formatUint8Array(v []uint8) string {
	return fmt.Sprintf("%x", v)
}

func getMd5(s string) string {
	v := md5.Sum([]byte(s))
	return fmt.Sprintf("%x", v)
}

func formatBool(v bool) string {
	if v {
		return "yes"
	}

	return "no"
}

func formatNullableTime(v interface{}) string {
	if v == nil {
		return ""
	}

	return formatTime(v.(time.Time))
}

func formatNullableByteArray(v interface{}) string {
	if v == nil {
		return ""
	}

	return formatUint8Array(v.([]uint8))
}

func formatNullableString(v interface{}) string {
	if v == nil {
		return ""
	}
	return v.(string)
}
