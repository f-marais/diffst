package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/smallfish/simpleyaml"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type STSide struct {
	rows    *sql.Rows
	cols    []*sql.ColumnType
	fuzzies []bool
	SQL     string
	db      *sql.DB
}

var S, T STSide
var y *simpleyaml.Yaml
var fuzzies []bool
var trace bool
var tcase *string
var verbose *bool
var columns *bool

const (
	batch_size   = 2000
	output_limit = 30
	env_root     = "_DIFFST_"
)

func setupdb(nd string, rs *STSide) {

	db_name, err := y.Get(nd).Get("DB").String()
	if err != nil {
		log.Fatal("Getting "+nd+" DB", err)
	}
	env_db_driver := env_root + "DB_" + db_name + "_DRIVER"
	driver_name, success := os.LookupEnv(env_db_driver)
	if !success {
		log.Fatal("Driver lookup: "+env_db_driver+
			" not found in environment", err)
	}
	url_name, success := os.LookupEnv(env_root + "DB_" + db_name + "_URL")
	if !success {
		log.Fatal("DB URL lookup:"+url_name+
			" not found in environment, expecting env var:"+
			env_root+"DB_"+db_name+"_URL"+
			"\n\tValues are DB specific eg: "+
			"\n\tPostgreSQL:\tuser=myuser dbname=somedb sslmode=disable"+
			"\n\tSQLite:\t/path/to/dbfile.db"+
			"\n\tMYSQL:\tuser:user@tcp(host:port)/somedb"+
			"\n\tError returned:", err)
	}
	if *verbose {
		fmt.Println(nd+" DB url is :", url_name)
	}
	// Open db
	rs.db, err = sql.Open(driver_name, url_name)
	if err != nil {
		log.Fatal("DB:", nd, ":", err)
	}
	// defer (*rs).db.Close()

	// Run the SQL
	rs.SQL, err = y.Get(nd).Get("SQL").String()
	if err != nil {
		log.Fatal(err)
	}

	rs.rows, err = rs.db.Query((*rs).SQL)
	if err != nil {
		log.Fatal(err)
	}
	// defer (*rs).rows.Close()

	// Get the column metadata
	rs.cols, err = rs.rows.ColumnTypes()
	if err != nil {
		log.Fatal(err)
	}
}

func cmp(s, t []string) bool {

Forloop:
	for i := 0; i < len(T.cols); i++ {
		if fuzzies[i] == true {
			aval, err := strconv.ParseFloat(s[i], 64)
			if err == nil {
				bval, err2 := strconv.ParseFloat(t[i], 64)
				if err2 == nil {
					if bval-aval >= 1 || aval-bval >= 1 {
						return false
					} else {
						continue Forloop
					}
				} else {
					fmt.Println("1not converting", s[i], ":", t[i], ":", err)
				}
			} else {
				fmt.Println("2not converting", s[i], ":", t[i], ":", err)
			}
		}
		// fmt.Println("src:", s[i])
		if s[i] != t[i] {
			return false
		}
	}
	return true
}

func pop_batch(d STSide, bs int) ([][]string, int) {

	var raw_values = make([][]string, bs)
	rowcount := 0
	for d.rows.Next() {
		var sonerow = make([]interface{}, len(d.cols))
		for i := 0; i < len(d.cols); i++ {
			var ii interface{}
			sonerow[i] = &ii
		}
		if err := d.rows.Scan(sonerow...); err != nil {
			log.Fatal(err)
		}
		raw_values[rowcount] = cols_to_str(sonerow, d.cols)
		rowcount = rowcount + 1
		if rowcount == bs {
			break
		}
	}
	return raw_values, rowcount
}

func cols_to_str(raw_values []interface{}, cols []*sql.ColumnType) []string {

	var disp_cols = make([]string, len(cols))
	tlen := 0
	for i, _ := range cols {
		var raw_value = *(raw_values[i].(*interface{}))
		switch t := raw_value.(type) {
		case int64:
			if value, ok := raw_value.(int64); ok {
				if trace {
					fmt.Println("found int")
				}
				disp_cols[i] = strconv.FormatInt(value, 10)
				if tlen < len(disp_cols[i]) {
					tlen = len(disp_cols[i])
				}
			}
		case float64:
			if value, ok := raw_value.(float64); ok {
				if trace {
					fmt.Println("found float")
				}
				disp_cols[i] = strconv.FormatFloat(value,
					'f', -1, 64)
				if tlen < len(disp_cols[i]) {
					tlen = len(disp_cols[i])
				}
			}
		case bool:
			if value, ok := raw_value.(bool); ok {
				if trace {
					fmt.Println("found bool")
				}
				disp_cols[i] = strconv.FormatBool(value)
				if tlen < len(disp_cols[i]) {
					tlen = len(disp_cols[i])
				}
			}
		case string:
			disp_cols[i] = fmt.Sprintf("%v", raw_value)
			if tlen < len(disp_cols[i]) {
				tlen = len(disp_cols[i])
			}
		case []byte:
			if a, ok := raw_value.([]uint8); ok {
				if trace {
					fmt.Println("found uint")
				}
				disp_cols[i] = string(a)
				if tlen < len(disp_cols[i]) {
					tlen = len(disp_cols[i])
				}
			}
			/* case time.Time:
			if value, ok := raw_value.(time.Time); ok {
				pvalue = value
			}   */
		default:
			var r = reflect.TypeOf(t)
			if trace {
				fmt.Printf("Other:%v\n", r)
			}
		}
	}
	return disp_cols
}

func disp_diff(src_outp, tgt_outp [][]string, disp_idx int) {
	if *columns {
		tlen := 30
		fmt.Println(strings.Repeat("-", 30) + "+" +
			strings.Repeat("-", tlen) + "+" +
			strings.Repeat("-", tlen) + "+")
		for i := 0; i < disp_idx; i++ {
			for j, _ := range T.cols {
				fmt.Print(fmt.Sprintf("%-29.29s", T.cols[j].Name()))
				if src_outp[i][j] != tgt_outp[i][j] {
					fmt.Print("*")
				} else {
					fmt.Print(" ")
				}
				fmt.Println("|" + src_outp[i][j] +
					strings.Repeat(" ", tlen-len(src_outp[i][j])) +
					"|" + tgt_outp[i][j] + strings.Repeat(" ",
					tlen-len(tgt_outp[i][j])) + "|")
			}
			fmt.Println(strings.Repeat("-", 30) + "+" +
				strings.Repeat("-", tlen) + "+" +
				strings.Repeat("-", tlen) + "+")
		}
	} else {
		for i := 0; i < disp_idx; i++ {
			src_outpl, tgt_outpl := "", ""
			src_empty, tgt_empty := true, true
			var diff_sign string
			for s, _ := range S.cols {
				if len(src_outp[i][s]) > 0 {
					src_empty = false
				}
				src_outpl += src_outp[i][s] + "|"
			}
			for t, _ := range T.cols {
				if len(tgt_outp[i][t]) > 0 {
					tgt_empty = false
				}
				tgt_outpl += tgt_outp[i][t] + "|"
			}
			if tgt_empty {
				diff_sign = "<"
				tgt_outpl = ""
			} else if src_empty {
				diff_sign = ">"
				src_outpl = ""
			} else {
				diff_sign = "|"
			}

			if _, err := fmt.Printf("%-80.80s %s %-80.80s\n",
				strings.Trim(src_outpl, "\\|"), diff_sign,
				strings.Trim(tgt_outpl, "\\|")); err != nil {
				log.Fatal(err)
			}
		}
	}
}

func main() {

	var success bool
	var tc_dir string
	_, trace = os.LookupEnv(env_root + "TRACE")
	tc_dir, success = os.LookupEnv(env_root + "TC_DIR")
	if !success {
		log.Fatal(env_root + "_TC_DIR not found in environment")
	}
	tcase = flag.String("t", "", "test case to run "+
		"-  eg for df0123.yml use 0123")
	verbose = flag.Bool("v", false, "Run verbosely - "+
		"producing helpful output")
	columns = flag.Bool("c", false, "Stack columns vertically in output")
	flag.Parse()
	x, err := ioutil.ReadFile(tc_dir + "/df" + (*tcase) + ".yml")
	if err != nil {
		log.Fatal("Reading yaml: ", err)
	}
	y, err = simpleyaml.NewYaml(x)
	if err != nil {
		log.Fatal("yaml:", err)
	}
	// Login to dbs, get source and target rows and col metadata
	var ends = map[string](*STSide){
		"Source": &S,
		"Target": &T,
	}
	for nd, rs := range ends {
		setupdb(nd, rs)
	}
	if len(S.cols) != len(T.cols) {
		log.Fatal("Number of columns being returned is different:" +
			strconv.Itoa(len(S.cols)) + " vs " +
			strconv.Itoa(len(T.cols)))
	}

	// make array of fuzzy matching columns
	fuzzies = make([]bool, len(T.cols))
	fuzzy, err := y.Get("Fuzzy").Array()
	if err != nil {
		// fmt.Println("Fuzzy not found:", err)
	} else {
		for i := 0; i < len(fuzzy); i++ {
			if _, ok := fuzzy[i].(string); ok {
				for j := 0; j < len(T.cols); j++ {
					if fuzzy[i] == T.cols[j].Name() {
						fuzzies[j] = true
					}
				}
			}
		}
	}

	// loop through the results batch_size at a time
	for {
		sraw_values, srow_count := pop_batch(S, batch_size)
		traw_values, trow_count := pop_batch(T, batch_size)

		// fmt.Println("fetching")
		//----------------------------------------------
		// Quick look - are there diffs?

		if trace {
			fmt.Println("srow:", srow_count, ",trow:", trow_count)
		}
		diffs := false
		if srow_count == trow_count {
			for i := 0; i < srow_count; i++ {
				if !cmp(sraw_values[i], traw_values[i]) {
					diffs = true
					// fmt.Println("not cmp", i)
					break
				}
			}
		}

		if !diffs && srow_count < batch_size-1 &&
			trow_count < batch_size-1 &&
			srow_count == trow_count {
			break
		}

		if !diffs && srow_count == trow_count {
			continue
		}

		//---------- Calculate lcs ----------------
		lengths := make([][]int, srow_count+1)
		for i := 0; i <= srow_count; i++ {
			lengths[i] = make([]int, trow_count+1)
		}
		// row 0 and column 0 are initialized to 0 already

		for i := 0; i < srow_count; i++ {
			for j := 0; j < trow_count; j++ {
				if cmp(sraw_values[i], traw_values[j]) {
					lengths[i+1][j+1] = lengths[i][j] + 1
				} else if lengths[i+1][j] > lengths[i][j+1] {
					lengths[i+1][j+1] = lengths[i+1][j]
				} else {
					lengths[i+1][j+1] = lengths[i][j+1]
				}
			}
		}

		// read the lcs out from the matrix
		// lcs := make([][]string, srow_count, srow_count)
		lcs := make([][]string, 0, lengths[srow_count][trow_count])
		sind := 0
		for x, y := srow_count, trow_count; x != 0 && y != 0; {
			if lengths[x][y] == lengths[x-1][y] {
				x--
			} else if lengths[x][y] == lengths[x][y-1] {
				y--
			} else {
				lcs = append(lcs, sraw_values[x-1])
				sind++
				x--
				y--
			}
		}
		// reverse lcs
		for i, j := 0, len(lcs)-1; i < j; i, j = i+1, j-1 {
			lcs[i], lcs[j] = lcs[j], lcs[i]
		}

		if trace {
			for i := 0; i < len(lcs); i = i + 1 {
				fmt.Println(lcs[i])
			}
		}
		// build output
		sr, tr, disp_idx, lcs_idx := 0, 0, 0, 0
		src_outp := make([][]string, output_limit, output_limit)
		tgt_outp := make([][]string, output_limit, output_limit)
		empty_cols := make([]string, len(S.cols), len(S.cols))
		for (sr < srow_count || tr < trow_count ||
			lcs_idx < sind) && disp_idx < output_limit {
			var src_eqls, tgt_eqls bool
			if trace {
				fmt.Println("sr:", sr, ", lcs_idx:", lcs_idx)
			}
			if sr < srow_count && len(lcs) > 0 && lcs_idx < sind {
				src_eqls = cmp(sraw_values[sr], lcs[lcs_idx])
			} else {
				src_eqls = false
			}

			if tr < trow_count && len(lcs) > 0 && lcs_idx < sind {
				tgt_eqls = cmp(traw_values[tr], lcs[lcs_idx])
			} else {
				tgt_eqls = false
			}
			if src_eqls && tgt_eqls {
				sr++
				tr++
				lcs_idx++
				continue
			}
			if trace {
				fmt.Println(disp_idx, ":", src_eqls, ":", tgt_eqls)
				fmt.Println("src:", sraw_values[sr])
				fmt.Println("tgt:", traw_values[tr])
				fmt.Println("lcs:", lcs[lcs_idx])
			}
			tgt_outp[disp_idx] = empty_cols
			src_outp[disp_idx] = empty_cols
			if !src_eqls {
				if sr < srow_count {
					src_outp[disp_idx] = sraw_values[sr]
					sr++
				}
			}
			if !tgt_eqls {
				if tr < trow_count {
					tgt_outp[disp_idx] = traw_values[tr]
					tr++
				}
			}
			disp_idx++
		}
		disp_diff(src_outp, tgt_outp, disp_idx)
		break
	}
}
