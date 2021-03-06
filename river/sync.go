package river

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
	"github.com/zeayes/go-mysql-elasticsearch/elastic"
)

const (
	syncInsertDoc = iota
	syncDeleteDoc
	syncUpdateDoc
)

const (
	fieldTypeList = "list"
	fieldTypeString = "string"
	// for the mysql int type to es date type
	// set the [rule.field] created_time = ",date"
	fieldTypeDate = "date"
	// transfer datetime to timestamp
	filedTypeTimestamp = "timestamp"
)

const mysqlDateFormat = "2006-01-02"

type posSaver struct {
	pos   mysql.Position
	force bool
}

type eventHandler struct {
	r *River
}

func (h *eventHandler) OnRotate(e *replication.RotateEvent) error {
	pos := mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}

	h.r.syncCh <- posSaver{pos, true}

	return h.r.ctx.Err()
}

func (h *eventHandler) OnTableChanged(schema, table string) error {
	err := h.r.updateRule(schema, table)
	if err != nil && err != ErrRuleNotExist {
		return errors.Trace(err)
	}
	return nil
}

func (h *eventHandler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	h.r.syncCh <- posSaver{nextPos, true}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnXID(nextPos mysql.Position) error {
	h.r.syncCh <- posSaver{nextPos, false}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	rule, ok := h.r.rules[ruleKey(e.Table.Schema, e.Table.Name)]
	if !ok {
		return nil
	}

	var reqs []*elastic.BulkRequest
	var err error
	switch e.Action {
	case canal.InsertAction:
		reqs, err = h.r.makeInsertRequest(rule, e.Rows)
	case canal.DeleteAction:
		reqs, err = h.r.makeDeleteRequest(rule, e.Rows)
	case canal.UpdateAction:
		reqs, err = h.r.makeUpdateRequest(rule, e.Rows)
	default:
		err = errors.Errorf("invalid rows action %s", e.Action)
	}

	if err != nil {
		h.r.cancel()
		return errors.Errorf("make %s ES request err %v, close sync", e.Action, err)
	}

	h.r.syncCh <- reqs

	return h.r.ctx.Err()
}

func (h *eventHandler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

func (h *eventHandler) OnPosSynced(pos mysql.Position, force bool) error {
	return nil
}

func (h *eventHandler) String() string {
	return "ESRiverEventHandler"
}

func (r *River) syncLoop() {
	bulkSize := r.c.BulkSize
	if bulkSize == 0 {
		bulkSize = 128
	}

	interval := r.c.FlushBulkTime.Duration
	if interval == 0 {
		interval = 200 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer r.wg.Done()

	lastSavedTime := time.Now()
	reqs := make([]*elastic.BulkRequest, 0, 1024)

	var pos mysql.Position

	for {
		needFlush := false
		needSavePos := false

		select {
		case v := <-r.syncCh:
			switch v := v.(type) {
			case posSaver:
				now := time.Now()
				if v.force || now.Sub(lastSavedTime) > 3*time.Second {
					lastSavedTime = now
					needFlush = true
					needSavePos = true
					pos = v.pos
				}
			case []*elastic.BulkRequest:
				reqs = append(reqs, v...)
				needFlush = len(reqs) >= bulkSize
			}
		case <-ticker.C:
			needFlush = true
		case <-r.ctx.Done():
			return
		}

		if needFlush {
			// TODO: retry some times?
			if err := r.doBulk(reqs); err != nil {
				log.Errorf("do ES bulk err %v, close sync", err)
				r.cancel()
				return
			}
			reqs = reqs[0:0]
		}

		if needSavePos {
			if err := r.master.Save(pos); err != nil {
				log.Errorf("save sync position %s err %v, close sync", pos, err)
				r.cancel()
				return
			}
		}
	}
}

// for insert and delete
func (r *River) makeRequest(rule *Rule, action string, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	reqs := make([]*elastic.BulkRequest, 0, len(rows))

	esAction := rule.ActionMapping[action]
	if esAction == "" {
		return nil, nil
	}

	for _, values := range rows {
		id, err := r.getDocID(rule, values)
		if err != nil {
			return nil, errors.Trace(err)
		}

		parentID := ""
		if len(rule.Parent) > 0 {
			if parentID, err = r.getParentID(rule, values, rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
		}

		if esAction == elastic.ActionDelete {
			req := &elastic.BulkRequest{
				Index:    rule.Index,
				Type:     rule.Type,
				ID:       id,
				Parent:   parentID,
				Pipeline: rule.Pipeline,
				Action:   elastic.ActionDelete,
			}
			r.st.DeleteNum.Add(1)
			reqs = append(reqs, req)
			continue
		}
		req := r.makeInsertReqData(rule, values, esAction, id, parentID)
		if req == nil {
			continue
		}
		if esAction == elastic.ActionIndex {
			r.st.InsertNum.Add(1)
		} else {
			r.st.UpdateNum.Add(1)
		}
		reqs = append(reqs, req)
	}

	return reqs, nil
}

func (r *River) makeInsertRequest(rule *Rule, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	return r.makeRequest(rule, canal.InsertAction, rows)
}

func (r *River) makeDeleteRequest(rule *Rule, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	return r.makeRequest(rule, canal.DeleteAction, rows)
}

func (r *River) makeUpdateRequest(rule *Rule, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	if len(rows)%2 != 0 {
		return nil, errors.Errorf("invalid update rows event, must have 2x rows, but %d", len(rows))
	}
	esAction := rule.ActionMapping[canal.UpdateAction]
	if esAction == "" {
		return nil, nil
	}
	reqs := make([]*elastic.BulkRequest, 0, len(rows))

	for i := 0; i < len(rows); i += 2 {
		beforeID, err := r.getDocID(rule, rows[i])
		if err != nil {
			return nil, errors.Trace(err)
		}

		afterID, err := r.getDocID(rule, rows[i+1])

		if err != nil {
			return nil, errors.Trace(err)
		}

		beforeParentID, afterParentID := "", ""
		if len(rule.Parent) > 0 {
			if beforeParentID, err = r.getParentID(rule, rows[i], rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
			if afterParentID, err = r.getParentID(rule, rows[i+1], rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
		}

		if beforeID != afterID || beforeParentID != afterParentID {
			req := &elastic.BulkRequest{
				Index:  rule.Index,
				Type:   rule.Type,
				ID:     beforeID,
				Parent: beforeParentID,
				Action: elastic.ActionDelete,
			}
			r.st.DeleteNum.Add(1)
			reqs = append(reqs, req)

			req = r.makeInsertReqData(rule, rows[i+1], elastic.ActionDelete, afterID, afterParentID)
			if req == nil {
				continue
			}
			r.st.InsertNum.Add(1)
			reqs = append(reqs, req)
			continue
		}
		var req *elastic.BulkRequest
		if len(rule.Pipeline) > 0 {
			req = r.makeInsertReqData(rule, rows[i+1], elastic.ActionIndex, beforeID, beforeParentID)
		} else {
			req = r.makeUpdateReqData(rule, rows[i], rows[i+1], beforeID, beforeParentID)
		}
		if req == nil {
			continue
		}
		r.st.UpdateNum.Add(1)
		reqs = append(reqs, req)
	}

	return reqs, nil
}

func (r *River) makeReqColumnData(col *schema.TableColumn, value interface{}) interface{} {
	switch col.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			// for binlog, ENUM may be int64, but for dump, enum is string
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				log.Warnf("invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
				return ""
			}

			return col.EnumValues[eNum]
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			// for binlog, SET may be int64, but for dump, SET is string
			bitmask := value
			sets := make([]string, 0, len(col.SetValues))
			for i, s := range col.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		}
	case schema.TYPE_BIT:
		switch value := value.(type) {
		case string:
			// for binlog, BIT is int64, but for dump, BIT is string
			// for dump 0x01 is for 1, \0 is for 0
			if value == "\x01" {
				return int64(1)
			}

			return int64(0)
		}
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case []byte:
			return string(value[:])
		}
	case schema.TYPE_JSON:
		var f interface{}
		var err error
		switch v := value.(type) {
		case string:
			err = json.Unmarshal([]byte(v), &f)
		case []byte:
			err = json.Unmarshal(v, &f)
		}
		if err == nil && f != nil {
			return f
		}
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		switch v := value.(type) {
		case string:
			vt, err := time.ParseInLocation(mysql.TimeFormat, string(v), time.Local)
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(time.RFC3339)
		}
	case schema.TYPE_DATE:
		switch v := value.(type) {
		case string:
			vt, err := time.Parse(mysqlDateFormat, string(v))
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(mysqlDateFormat)
		}
	}

	return value
}

func (r *River) getFieldParts(k string, v string) (string, string, string) {
	composedField := strings.Split(v, ",")

	mysql := k
	elastic := composedField[0]
	fieldType := ""

	if 0 == len(elastic) {
		elastic = mysql
	}
	if 2 == len(composedField) {
		fieldType = composedField[1]
	}

	return mysql, elastic, fieldType
}

func (r *River) makeFieldData(rule *Rule, values []interface{}) map[string]interface{}  {
	data := make(map[string]interface{}, len(rule.FieldMapping))
	for key, value := range rule.FieldMapping {
		esField, mysqlField, fieldType := r.getFieldParts(key, value)
		i := rule.TableFields[mysqlField]
		c := rule.TableInfo.Columns[i]
		var value interface{}
		if fieldType == "" {
			value = r.makeReqColumnData(&c, values[i])
		} else {
			value = r.getFieldValue(&c, fieldType, values[i])
		}
		_, pass := rule.CheckWhere(c.Name, value)
		if !pass {
			return nil
		}
		data[esField] = value
	}
	return data
}

func (r *River) makeInsertReqData(rule *Rule, values []interface{}, action, id, parentID string) *elastic.BulkRequest {
	data := r.makeFieldData(rule, values)
	if data == nil {
		return nil
	}

	return &elastic.BulkRequest{
		Index:    rule.Index,
		Type:     rule.Type,
		ID:       id,
		Parent:   parentID,
		Pipeline: rule.Pipeline,
		Action:   action,
		Data:     data,
	}
}

func (r *River) makeUpdateReqData(rule *Rule, beforeValues []interface{}, afterValues []interface{}, id, parentID string) *elastic.BulkRequest {
	req := &elastic.BulkRequest{
		Index:    rule.Index,
		Type:     rule.Type,
		ID:       id,
		Parent:   parentID,
		Pipeline: rule.Pipeline,
		Action:   elastic.ActionUpdate,
		Data: make(map[string]interface{}, len(beforeValues)),
	}
	for i, c := range rule.TableInfo.Columns {
		exist, pass := rule.CheckWhere(c.Name, r.makeReqColumnData(&c, afterValues[i]))
		if exist && !pass {
			req.Action = elastic.ActionDelete
			return req
		}
		if exist && !reflect.DeepEqual(afterValues[i], beforeValues[i]) {
			req.Action = elastic.ActionIndex
		}
	}

	afterData := r.makeFieldData(rule, afterValues)
	if afterData == nil {
		return nil
	}
	beforeData := r.makeFieldData(rule, beforeValues)
	for key, value := range afterData {
		v, ok := beforeData[key]
		if ok && reflect.DeepEqual(value, v) && req.Action != elastic.ActionIndex {
			continue
		}
		req.Data[key] = value
	}
	if len(req.Data) == 0 {
		return nil
	}
	return req
}

// If id in toml file is none, get primary keys in one row and format them into a string, and PK must not be nil
// Else get the ID's column in one row and format them into a string
func (r *River) getDocID(rule *Rule, row []interface{}) (string, error) {
	var (
		ids []interface{}
		err error
	)
	if rule.ID == nil {
		ids, err = rule.TableInfo.GetPKValues(row)
		if err != nil {
			return "", err
		}
	} else {
		ids = make([]interface{}, 0, len(rule.ID))
		for _, column := range rule.ID {
			value, err := rule.TableInfo.GetColumnValue(column, row)
			if err != nil {
				return "", err
			}
			ids = append(ids, value)
		}
	}

	var buf bytes.Buffer

	sep := ""
	for i, value := range ids {
		if value == nil {
			return "", errors.Errorf("The %ds id or PK value is nil", i)
		}

		buf.WriteString(fmt.Sprintf("%s%v", sep, value))
		sep = ":"
	}

	return buf.String(), nil
}

func (r *River) getParentID(rule *Rule, row []interface{}, columnName string) (string, error) {
	index := rule.TableInfo.FindColumn(columnName)
	if index < 0 {
		return "", errors.Errorf("parent id not found %s(%s)", rule.TableInfo.Name, columnName)
	}

	return fmt.Sprint(row[index]), nil
}

func (r *River) doBulk(reqs []*elastic.BulkRequest) error {
	if len(reqs) == 0 {
		return nil
	}
	if resp, err := r.es.Bulk(reqs); err != nil {
		log.Errorf("sync docs err %v after binlog %s", err, r.canal.SyncedPosition())
		return errors.Trace(err)
	} else if resp.Code/100 == 2 || resp.Errors {
		for i := 0; i < len(resp.Items); i++ {
			for action, item := range resp.Items[i] {
				if len(item.Error) > 0 {
					log.Errorf("%s index: %s, type: %s, id: %s, status: %d, error: %s",
						action, item.Index, item.Type, item.ID, item.Status, item.Error)
				}
			}
		}
	}

	return nil
}

// get mysql field value and convert it to specific value to es
func (r *River) getFieldValue(col *schema.TableColumn, fieldType string, value interface{}) interface{} {
	var fieldValue interface{}
	switch fieldType {
	case fieldTypeList:
		v := r.makeReqColumnData(col, value)
		if str, ok := v.(string); ok {
			fieldValue = strings.Split(str, ",")
		} else {
			fieldValue = v
		}
	case fieldTypeString:
		fieldValue = value.(string)
	case fieldTypeDate:
		if col.Type == schema.TYPE_NUMBER {
			col.Type = schema.TYPE_DATETIME

			v := reflect.ValueOf(value)
			switch v.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				fieldValue = r.makeReqColumnData(col, time.Unix(v.Int(), 0).Format(mysql.TimeFormat))
			}
		}
	case filedTypeTimestamp:
		if col.Type == schema.TYPE_DATE {
			ts, err := time.ParseInLocation(mysqlDateFormat, value.(string), time.Local)
			if err != nil {
				log.Errorf("parse field %s to timestamp fail %v", col.Name, err)
				fieldValue = value
			}
			fieldValue = ts.Unix()
		} else if col.Type == schema.TYPE_DATETIME {
			ts, err := time.ParseInLocation(mysql.TimeFormat, value.(string), time.Local)
			if err != nil {
				log.Errorf("parse field %s to timestamp fail %v", col.Name, err)
				fieldValue = value
			}
			fieldValue = ts.Unix()
		}
	}

	if fieldValue == nil {
		fieldValue = r.makeReqColumnData(col, value)
	}
	return fieldValue
}
