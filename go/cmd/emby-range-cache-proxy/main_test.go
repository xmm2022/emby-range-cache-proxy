package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckConfigCommandLoadsConfigAndExits(t *testing.T) {
	configPath := writeCommandConfig(t, `{
		"emby_base_url": "http://127.0.0.1:8096",
		"cache_dir": "`+filepath.ToSlash(filepath.Join(t.TempDir(), "cache"))+`"
	}`)
	cmd := exec.Command("go", "run", ".", "--config", configPath, "--check-config")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\n%s", err, output)
	}
	if string(output) != "config ok\n" {
		t.Fatalf("output=%q", output)
	}
}

func TestCheckConfigCommandRejectsInvalidConfig(t *testing.T) {
	configPath := writeCommandConfig(t, `{
		"emby_base_url": "http://127.0.0.1:8096",
		"cache_dir": "`+filepath.ToSlash(filepath.Join(t.TempDir(), "cache"))+`",
		"listen_port": 70000
	}`)
	cmd := exec.Command("go", "run", ".", "--config", configPath, "--check-config")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("command unexpectedly succeeded: %s", output)
	}
	if !strings.Contains(string(output), "load config failed:") {
		t.Fatalf("output=%q", output)
	}
}

func writeCommandConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
