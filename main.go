package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/exp/rand"
)

const (
	dbUser     = "test_user"
	dbPassword = "test_pass"
	dbName     = "test_db"
	dbHost     = "127.0.0.1"
	dbPort     = "3306"
)

func main() {
	// MySQLに接続
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		dbUser, dbPassword, dbHost, dbPort, dbName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// テーブルを削除
	_, err = db.Exec("DROP TABLE IF EXISTS store_bitflag")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("DROP TABLE IF EXISTS store_holiday")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("DROP TABLE IF EXISTS store")
	if err != nil {
		log.Fatal(err)
	}

	// テーブル作成
	createTables(db)

	// データ挿入
	numStores := 1000000 // 100万店舗をテスト
	fmt.Println("Inserting data...")
	insertTestData(db, numStores)

	// パフォーマンステスト
	fmt.Println("\nRunning performance tests...")
	benchmarkSelectAll(db)
	benchmarkSelectMonday(db)
}

func createTables(db *sql.DB) {
	// 店舗テーブル（store）
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS store (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	// 定休日テーブル（store_holiday）
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS store_holiday (
			id INT AUTO_INCREMENT PRIMARY KEY,
			store_id INT NOT NULL,
			day_of_week ENUM('Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday') NOT NULL,
			FOREIGN KEY (store_id) REFERENCES store(id) ON DELETE CASCADE
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	// ビットフラグテーブル（store_bitflag）
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS store_bitflag (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			holidays INT NOT NULL
		)
	`)
	if err != nil {
		log.Fatal(err)
	}
}

func insertTestData(db *sql.DB, numStores int) {
	// ランダムシードを設定
	rand.Seed(uint64(time.Now().UnixNano()))

	// 曜日のリスト
	daysOfWeek := []string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}

	// 別テーブル（正規化）
	start := time.Now()
	tx, _ := db.Begin()
	for i := 1; i <= numStores; i++ {
		// 店舗を追加
		_, _ = tx.Exec("INSERT INTO store (name) VALUES (?)", fmt.Sprintf("Store %d", i))
		storeID := i

		// ランダムに定休日の数を決定（1〜7個）
		numHolidays := rand.Intn(7) + 1
		selectedDays := randomDays(daysOfWeek, numHolidays)

		// 選ばれた定休日を `store_holiday` にINSERT
		for _, day := range selectedDays {
			_, _ = tx.Exec("INSERT INTO store_holiday (store_id, day_of_week) VALUES (?, ?)", storeID, day)
		}

		// 選ばれた曜日のビットフラグを作成し、ビットフラグテーブルにも同じデータを格納
		holidays := calculateBitFlag(selectedDays)
		_, _ = tx.Exec("INSERT INTO store_bitflag (name, holidays) VALUES (?, ?)", fmt.Sprintf("Store %d", i), holidays)

		if i%10000 == 0 {
			fmt.Print("現在:", i)
		}
	}
	tx.Commit()
	fmt.Printf("INSERT (別テーブル & ビットフラグ) 完了: %v\n", time.Since(start))
}

// 指定された曜日リストからランダムに `n` 個の曜日を選ぶ
func randomDays(days []string, n int) []string {
	shuffledDays := append([]string{}, days...) // コピーを作成
	rand.Shuffle(len(shuffledDays), func(i, j int) { shuffledDays[i], shuffledDays[j] = shuffledDays[j], shuffledDays[i] })
	return shuffledDays[:n]
}

// 曜日リストを整数ビットフラグに変換
func calculateBitFlag(selectedDays []string) int {
	dayToBit := map[string]int{
		"Sunday": 1, "Monday": 2, "Tuesday": 4, "Wednesday": 8,
		"Thursday": 16, "Friday": 32, "Saturday": 64,
	}

	holidays := 0
	for _, day := range selectedDays {
		holidays |= dayToBit[day]
	}
	return holidays
}

func benchmarkSelectAll(db *sql.DB) {
	// 全店舗の定休日を取得
	start := time.Now()
	_, _ = db.Query("SELECT s.id, s.name, GROUP_CONCAT(h.day_of_week) FROM store s LEFT JOIN store_holiday h ON s.id = h.store_id GROUP BY s.id")
	fmt.Printf("SELECT ALL (別テーブル) 完了: %v\n", time.Since(start))

	start = time.Now()
	_, _ = db.Query("SELECT id, name, holidays FROM store_bitflag")
	fmt.Printf("SELECT ALL (ビットフラグ) 完了: %v\n", time.Since(start))
}

func benchmarkSelectMonday(db *sql.DB) {
	// 特定の曜日（Monday）の定休日店舗を取得
	start := time.Now()
	_, _ = db.Query("SELECT s.* FROM store s JOIN store_holiday h ON s.id = h.store_id WHERE h.day_of_week = 'Monday'")
	fmt.Printf("SELECT WHERE Monday (別テーブル) 完了: %v\n", time.Since(start))

	start = time.Now()
	_, _ = db.Query("SELECT * FROM store_bitflag WHERE (holidays & 2) > 0")
	fmt.Printf("SELECT WHERE Monday (ビットフラグ) 完了: %v\n", time.Since(start))
}
