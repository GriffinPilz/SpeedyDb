package structuredDB

import (
	"database/sql"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type Column struct {
	Name     string
	DataType string
	ColType  string
	CharMax  sql.NullInt64
	Charset  sql.NullString
	NumPrec  sql.NullInt64
	NumScale sql.NullInt64
	DtPrec   sql.NullInt64
}

func fracBytes(fsp int64) int64 {
	switch {
	case fsp <= 0:
		return 0
	case fsp <= 2:
		return 1
	case fsp <= 4:
		return 2
	default:
		return 3
	}
}

func decimalBytes(precision, scale int64) int64 {
	intDigits := precision - scale
	if intDigits < 0 {
		intDigits = 0
	}

	bytesForDigits := func(d int64) int64 {
		full := d / 9
		rem := d % 9
		b := full * 4
		switch {
		case rem == 0:
		case rem <= 2:
			b += 1
		case rem <= 4:
			b += 2
		case rem <= 6:
			b += 3
		default:
			b += 4
		}
		return b
	}

	return bytesForDigits(intDigits) + bytesForDigits(scale)
}

var reEnumSet = regexp.MustCompile(`^(enum|set)\((.*)\)$`)

func countEnumOrSetMembers(colType string) (int, bool) {
	ct := strings.ToLower(strings.TrimSpace(colType))
	m := reEnumSet.FindStringSubmatch(ct)
	if m == nil {
		return 0, false
	}
	inside := m[2]

	inQuote := false
	escape := false
	count := 0
	tokenHasContent := false

	for _, r := range inside {
		if escape {
			escape = false
			tokenHasContent = true
			continue
		}
		if r == '\\' && inQuote {
			escape = true
			tokenHasContent = true
			continue
		}
		if r == '\'' {
			inQuote = !inQuote
			tokenHasContent = true
			continue
		}
		if r == ',' && !inQuote {
			if tokenHasContent {
				count++
			}
			tokenHasContent = false
			continue
		}
		if !strings.ContainsRune(" \t\r\n", r) {
			tokenHasContent = true
		}
	}
	if tokenHasContent {
		count++
	}
	return count, true
}

func MaxBytesForColumn(c Column, charsetMaxlen map[string]int64, ignoreJSON bool) (bytes int, ignored bool, err error) {
	dt := strings.ToLower(strings.TrimSpace(c.DataType))

	if ignoreJSON && dt == "json" {
		return 0, true, nil
	}

	switch dt {
	case "tinyint":
		return 1, false, nil
	case "smallint":
		return 2, false, nil
	case "mediumint":
		return 3, false, nil
	case "int", "integer":
		return 4, false, nil
	case "bigint":
		return 8, false, nil

	case "float":
		return 4, false, nil
	case "double", "real":
		return 8, false, nil

	case "decimal", "numeric":
		if c.NumPrec.Valid && c.NumScale.Valid {
			return int(decimalBytes(c.NumPrec.Int64, c.NumScale.Int64)), false, nil
		}
		return 0, false, fmt.Errorf("decimal missing precision/scale for column %q", c.Name)

	case "date":
		return 3, false, nil
	case "year":
		return 1, false, nil
	case "time":
		fsp := int64(0)
		if c.DtPrec.Valid {
			fsp = c.DtPrec.Int64
		}
		return int(3 + fracBytes(fsp)), false, nil
	case "datetime":
		fsp := int64(0)
		if c.DtPrec.Valid {
			fsp = c.DtPrec.Int64
		}
		return int(5 + fracBytes(fsp)), false, nil
	case "timestamp":
		fsp := int64(0)
		if c.DtPrec.Valid {
			fsp = c.DtPrec.Int64
		}
		return int(4 + fracBytes(fsp)), false, nil

	case "enum":
		if n, ok := countEnumOrSetMembers(c.ColType); ok {
			if n <= 255 {
				return 1, false, nil
			}
			return 2, false, nil
		}
		return 0, false, fmt.Errorf("could not parse enum members for %q: %s", c.Name, c.ColType)

	case "set":
		if n, ok := countEnumOrSetMembers(c.ColType); ok {
			return int(math.Ceil(float64(n) / 8.0)), false, nil
		}
		return 0, false, fmt.Errorf("could not parse set members for %q: %s", c.Name, c.ColType)

	case "char", "varchar":
		if !c.CharMax.Valid {
			return 0, false, fmt.Errorf("%s missing length for column %q", dt, c.Name)
		}

		bpc := int64(4)
		if c.Charset.Valid {
			if v, ok := charsetMaxlen[strings.ToLower(c.Charset.String)]; ok {
				bpc = v
			}
		}

		maxDataBytes := c.CharMax.Int64 * bpc

		if dt == "varchar" {
			lenBytes := int64(1)
			if c.CharMax.Int64 > 255 {
				lenBytes = 2
			}
			return int(maxDataBytes + lenBytes), false, nil
		}
		return int(maxDataBytes), false, nil

	case "binary":
		if !c.CharMax.Valid {
			return 0, false, fmt.Errorf("binary missing length for column %q", c.Name)
		}
		return int(c.CharMax.Int64), false, nil

	case "varbinary":
		if !c.CharMax.Valid {
			return 0, false, fmt.Errorf("varbinary missing length for column %q", c.Name)
		}
		lenBytes := int64(1)
		if c.CharMax.Int64 > 255 {
			lenBytes = 2
		}
		return int(c.CharMax.Int64 + lenBytes), false, nil

	case "tinyblob", "tinytext":
		return 255 + 1, false, nil
	case "blob", "text":
		return 65535 + 2, false, nil
	case "mediumblob", "mediumtext":
		return 16777215 + 3, false, nil
	case "longblob", "longtext":
		return int(4294967295 + 4), false, nil

	case "json":
		return 0, false, fmt.Errorf("json sizing not supported (set ignoreJSON=true) for column %q", c.Name)
	}

	ct := strings.ToLower(strings.TrimSpace(c.ColType))
	if strings.HasPrefix(ct, "bit(") && strings.HasSuffix(ct, ")") {
		mStr := strings.TrimSuffix(strings.TrimPrefix(ct, "bit("), ")")
		if m, e := strconv.ParseInt(mStr, 10, 64); e == nil && m > 0 {
			return int((m + 7) / 8), false, nil
		}
	}

	return 0, false, fmt.Errorf("unhandled type %q for column %q (column_type=%q)", dt, c.Name, c.ColType)
}

func ColumnSizeMap(db *sql.DB, schema, table string, ignoreJSON bool) (map[string]int, []string, error) {
	charsetMaxlen := map[string]int64{}
	{
		rows, err := db.Query(`SELECT character_set_name, maxlen FROM information_schema.character_sets`)
		if err != nil {
			return nil, nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var n string
			var ml int64
			if err := rows.Scan(&n, &ml); err != nil {
				return nil, nil, err
			}
			charsetMaxlen[strings.ToLower(n)] = ml
		}
		if err := rows.Err(); err != nil {
			return nil, nil, err
		}
	}

	q := `
SELECT
  column_name,
  data_type,
  column_type,
  character_maximum_length,
  character_set_name,
  numeric_precision,
  numeric_scale,
  datetime_precision
FROM information_schema.columns
WHERE table_schema=? AND table_name=?
ORDER BY ordinal_position;
`
	rows, err := db.Query(q, schema, table)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	out := make(map[string]int)
	var orderSlice []string
	for rows.Next() {
		var c Column
		if err := rows.Scan(
			&c.Name, &c.DataType, &c.ColType,
			&c.CharMax, &c.Charset,
			&c.NumPrec, &c.NumScale,
			&c.DtPrec,
		); err != nil {
			return nil, nil, err
		}

		b, ignored, err := MaxBytesForColumn(c, charsetMaxlen, ignoreJSON)
		if err != nil {
			return nil, nil, err
		}
		if ignored {
			continue
		}
		out[c.Name] = b
		orderSlice = append(orderSlice, c.Name)
	}
	return out, orderSlice, rows.Err()
}

func GetRowSizeSQL(user, password, host, port, schema, table string) (rowSizeBytes uint64, colSizes map[string]int, orderSlice []string, err error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/information_schema?parseTime=true", user, password, host, port)
	fmt.Println(dsn)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return 0, nil, nil, err
	}
	defer db.Close()

	ignoreJSON := true

	m, o, err := ColumnSizeMap(db, schema, table, ignoreJSON)
	if err != nil {
		return 0, nil, nil, err
	}

	var total uint64
	for _, sz := range m {
		total += uint64(sz)
	}

	return total, m, o, nil
}
