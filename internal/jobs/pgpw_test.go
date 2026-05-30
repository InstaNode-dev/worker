package jobs

import (
	"strings"
	"testing"
)

// TestSplitPGPassword pins the SEC-WORKER FINDING-1 + FINDING-2 fix:
// pg_dump call sites move the Postgres password from process argv into
// PGPASSWORD env. The helper must:
//   1. Strip the password from a typical userinfo URL.
//   2. Pass through unchanged when there is no password (cert auth,
//      no user, malformed URL with fail-open).
//   3. Never leak the literal password in the returned URL.
func TestSplitPGPassword(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		wantURL string
		wantPW  string
		wantErr bool
	}{
		{
			name:    "userpass",
			in:      "postgres://doadmin:abc123@host:25060/db?sslmode=require",
			wantURL: "postgres://doadmin@host:25060/db?sslmode=require",
			wantPW:  "abc123",
		},
		{
			name:    "user_only_no_password",
			in:      "postgres://doadmin@host:25060/db?sslmode=require",
			wantURL: "postgres://doadmin@host:25060/db?sslmode=require",
			wantPW:  "",
		},
		{
			name:    "no_userinfo",
			in:      "postgres://host:25060/db",
			wantURL: "postgres://host:25060/db",
			wantPW:  "",
		},
		{
			name:    "empty",
			in:      "",
			wantURL: "",
			wantPW:  "",
		},
		{
			name:    "percent_encoded_password",
			in:      "postgres://u:p%40ss%40word@h:5432/db",
			wantURL: "postgres://u@h:5432/db",
			wantPW:  "p@ss@word", // url.Userinfo.Password() decodes
		},
		{
			name:    "malformed_url_fail_open",
			in:      "::::not a url",
			wantURL: "::::not a url",
			wantPW:  "",
			wantErr: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotURL, gotPW, err := splitPGPassword(c.in)
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, c.wantErr)
			}
			if gotURL != c.wantURL {
				t.Errorf("URL\n  got:  %q\n  want: %q", gotURL, c.wantURL)
			}
			if gotPW != c.wantPW {
				t.Errorf("password\n  got:  %q\n  want: %q", gotPW, c.wantPW)
			}
		})
	}
}

// TestSplitPGPassword_NoLeak: the literal password substring must never
// appear in the returned URL (this is THE point of the fix).
func TestSplitPGPassword_NoLeak(t *testing.T) {
	const secret = "ZZZ_NEVER_IN_URL_ZZZ"
	in := "postgres://admin:" + secret + "@host:5432/db?sslmode=require"
	gotURL, gotPW, err := splitPGPassword(in)
	if err != nil {
		t.Fatalf("splitPGPassword(%q) error: %v", in, err)
	}
	if strings.Contains(gotURL, secret) {
		t.Errorf("returned URL %q still contains the password %q", gotURL, secret)
	}
	if gotPW != secret {
		t.Errorf("password\n  got:  %q\n  want: %q", gotPW, secret)
	}
}
