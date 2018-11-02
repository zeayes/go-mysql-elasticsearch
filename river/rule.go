package river

import (
	"reflect"
	"strings"

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/schema"
	"github.com/zeayes/go-mysql-elasticsearch/elastic"
)

var ElasticActions = map[string]string{
	elastic.ActionIndex:  canal.InsertAction,
	elastic.ActionUpdate: canal.UpdateAction,
	elastic.ActionDelete: canal.DeleteAction,
}
var DefaultActionMapping = map[string]string{
	canal.InsertAction: elastic.ActionIndex,
	canal.UpdateAction: elastic.ActionUpdate,
	canal.DeleteAction: elastic.ActionDelete,
}

// Rule is the rule for how to sync data from MySQL to ES.
// If you want to sync MySQL data into elasticsearch, you must set a rule to let use know how to do it.
// The mapping rule may thi: schema + table <-> index + document type.
// schema and table is for MySQL, index and document type is for Elasticsearch.
type Rule struct {
	Schema string   `toml:"schema"`
	Table  string   `toml:"table"`
	Index  string   `toml:"index"`
	Type   string   `toml:"type"`
	Parent string   `toml:"parent"`
	ID     []string `toml:"id"`

	Where map[string]interface{} `toml:"where"`

	// Default, a MySQL table field name is mapped to Elasticsearch field name.
	// Sometimes, you want to use different name, e.g, the MySQL file name is title,
	// but in Elasticsearch, you want to name it my_title.
	FieldMapping map[string]string `toml:"field"`

	ActionMapping map[string]string `toml:"action"`

	// MySQL table information
	TableInfo *schema.Table

	TableFields map[string]int

	//only MySQL fields in filter will be synced , default sync all fields
	Filter []string `toml:"filter"`

	// Elasticsearch pipeline
	// To pre-process documents before indexing
	Pipeline string `toml:"pipeline"`
}

func newDefaultRule(schema string, table string) *Rule {
	r := new(Rule)

	r.Schema = schema
	r.Table = table

	lowerTable := strings.ToLower(table)
	r.Index = lowerTable
	r.Type = lowerTable

	r.Where = make(map[string]interface{})
	r.TableFields = make(map[string]int)
	r.FieldMapping = make(map[string]string)
	r.ActionMapping = make(map[string]string)

	return r
}

func (r *Rule) prepare() error {
	if r.TableFields == nil {
		r.TableFields = make(map[string]int)
	}
	if r.FieldMapping == nil {
		r.FieldMapping = make(map[string]string)
	}

	if r.Where == nil {
		r.Where = make(map[string]interface{})
	}

	if r.ActionMapping == nil {
		r.ActionMapping = make(map[string]string, len(DefaultActionMapping))
	}
	for action, esAction := range DefaultActionMapping {
		if _, ok := r.ActionMapping[action]; !ok {
			r.ActionMapping[action] = esAction
		}
	}

	if len(r.Index) == 0 {
		r.Index = r.Table
	}

	if len(r.Type) == 0 {
		r.Type = r.Index
	}

	// ES must use a lower-case Type
	// Here we also use for Index
	r.Index = strings.ToLower(r.Index)
	r.Type = strings.ToLower(r.Type)

	return nil
}

// CheckFilter checkers whether the field needs to be filtered.
func (r *Rule) CheckFilter(field string) bool {
	if r.Filter == nil {
		return true
	}

	for _, f := range r.Filter {
		if f == field {
			return true
		}
	}
	return false
}

func (r *Rule) CheckWhere(field string, value interface{}) (bool, bool) {
	val, ok := r.Where[field]
	if !ok {
		return false, true
	}
	// 配置过该字段值，或者值相等，表示需要同步到ES
	return true, !ok || reflect.DeepEqual(val, value)
}
