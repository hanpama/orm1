package orm1

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseSQL(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		args []any
		want SQL
	}{
		{
			name: "basic single parameter",
			sql:  "SELECT * FROM users WHERE id = ?",
			args: []any{123},
			want: SQLFragment{Els: []SQL{
				SQLText{Text: "SELECT * FROM users WHERE id = "},
				SQLParam{Value: 123},
			}},
		},
		{
			name: "multiple parameters",
			sql:  "SELECT * FROM users WHERE age > ? AND name = ?",
			args: []any{25, "Alice"},
			want: SQLFragment{Els: []SQL{
				SQLText{Text: "SELECT * FROM users WHERE age > "},
				SQLParam{Value: 25},
				SQLText{Text: " AND name = "},
				SQLParam{Value: "Alice"},
			}},
		},
		{
			name: "question mark in single quotes",
			sql:  "SELECT * FROM users WHERE name = '?' AND age = ?",
			args: []any{30},
			want: SQLFragment{Els: []SQL{
				SQLText{Text: "SELECT * FROM users WHERE name = '?' AND age = "},
				SQLParam{Value: 30},
			}},
		},
		{
			name: "question mark in double quotes",
			sql:  `SELECT * FROM users WHERE column = "?" AND age = ?`,
			args: []any{40},
			want: SQLFragment{Els: []SQL{
				SQLText{Text: `SELECT * FROM users WHERE column = "?" AND age = `},
				SQLParam{Value: 40},
			}},
		},
		{
			name: "mixed quotes",
			sql:  `SELECT * FROM users WHERE a = '?' AND b = "?" AND c = ?`,
			args: []any{50},
			want: SQLFragment{Els: []SQL{
				SQLText{Text: `SELECT * FROM users WHERE a = '?' AND b = "?" AND c = `},
				SQLParam{Value: 50},
			}},
		},
		{
			name: "nested quotes",
			sql:  `SELECT * FROM users WHERE a = 'it''s a "test?"' AND b = ?`,
			args: []any{60},
			want: SQLFragment{Els: []SQL{
				SQLText{Text: `SELECT * FROM users WHERE a = 'it''s a "test?"' AND b = `},
				SQLParam{Value: 60},
			}},
		},
		{
			name: "empty string",
			sql:  "SELECT * FROM users WHERE name = '' AND age = ?",
			args: []any{70},
			want: SQLFragment{Els: []SQL{
				SQLText{Text: "SELECT * FROM users WHERE name = '' AND age = "},
				SQLParam{Value: 70},
			}},
		},
		{
			name: "question mark in string with escape",
			sql:  `SELECT * FROM users WHERE info = 'What''s your name?' AND id = ?`,
			args: []any{80},
			want: SQLFragment{Els: []SQL{
				SQLText{Text: `SELECT * FROM users WHERE info = 'What''s your name?' AND id = `},
				SQLParam{Value: 80},
			}},
		},
		{
			name: "no parameters",
			sql:  "SELECT * FROM users",
			args: []any{},
			want: SQLFragment{Els: []SQL{
				SQLText{Text: "SELECT * FROM users"},
			}},
		},
		{
			name: "multiple consecutive question marks",
			sql:  "INSERT INTO users (name, age, email) VALUES (?, ?, ?)",
			args: []any{"Bob", 25, "bob@example.com"},
			want: SQLFragment{Els: []SQL{
				SQLText{Text: "INSERT INTO users (name, age, email) VALUES ("},
				SQLParam{Value: "Bob"},
				SQLText{Text: ", "},
				SQLParam{Value: 25},
				SQLText{Text: ", "},
				SQLParam{Value: "bob@example.com"},
				SQLText{Text: ")"},
			}},
		},
		{
			name: "question mark at end",
			sql:  "SELECT * FROM users WHERE id = ?",
			args: []any{999},
			want: SQLFragment{Els: []SQL{
				SQLText{Text: "SELECT * FROM users WHERE id = "},
				SQLParam{Value: 999},
			}},
		},
		{
			name: "question mark at start",
			sql:  "? = id",
			args: []any{123},
			want: SQLFragment{Els: []SQL{
				SQLParam{Value: 123},
				SQLText{Text: " = id"},
			}},
		},
		{
			name: "nil parameter",
			sql:  "SELECT * FROM users WHERE deleted_at = ?",
			args: []any{nil},
			want: SQLFragment{Els: []SQL{
				SQLText{Text: "SELECT * FROM users WHERE deleted_at = "},
				SQLParam{Value: nil},
			}},
		},
		{
			name: "boolean parameters",
			sql:  "SELECT * FROM users WHERE is_active = ? AND is_admin = ?",
			args: []any{true, false},
			want: SQLFragment{Els: []SQL{
				SQLText{Text: "SELECT * FROM users WHERE is_active = "},
				SQLParam{Value: true},
				SQLText{Text: " AND is_admin = "},
				SQLParam{Value: false},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseSQL(tt.sql, tt.args...)

			if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(
				SQLFragment{},
				SQLText{},
				SQLParam{},
			)); diff != "" {
				t.Errorf("ParseSQL() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestParseSQLErrors(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "too few parameters",
			sql:  "SELECT * FROM users WHERE age = ? AND name = ?",
			args: []any{25},
		},
		{
			name: "too many parameters",
			sql:  "SELECT * FROM users WHERE age = ?",
			args: []any{25, "extra"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Error("Expected panic but did not panic")
				}
			}()

			ParseSQL(tt.sql, tt.args...)
		})
	}
}
